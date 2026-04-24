#![allow(
    clippy::indexing_slicing,
    clippy::nonminimal_bool,
    clippy::shadow_unrelated,
    clippy::too_many_arguments
)]

mod support;

use std::{
    error::Error,
    fs::{create_dir_all, read, write as write_file},
    io::{Error as IoError, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::Path,
    process::{Command, Output, Stdio},
    time::Duration,
};

use reqwest::Client;
use serde_json::{Value, json, to_vec};
use sha2::{Digest, Sha256};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_server::{ServerConfig, ServerError, ServerFrontend, serve_with_listener};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, task::JoinHandle, time::sleep};

type TestError = Box<dyn Error + Send + Sync>;

struct FrontendRuntime {
    _storage: tempfile::TempDir,
    base_url: String,
    host_port: String,
    server: JoinHandle<Result<(), ServerError>>,
}

impl FrontendRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn host_port(&self) -> &str {
        &self.host_port
    }
}

impl Drop for FrontendRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_git_lfs_push_and_pull_flow_works_against_shardline_lfs_frontend() {
    if !(command_available("git") && command_available("git-lfs")) {
        return;
    }

    let result = exercise_native_git_lfs_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native git-lfs e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_git_lfs_pull_and_fetch_all_work_against_shardline_lfs_frontend() {
    if !(command_available("git") && command_available("git-lfs")) {
        return;
    }

    let result = exercise_native_git_lfs_pull_and_fetch_all_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native git-lfs pull/fetch-all e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_bazel_remote_cache_flow_works_against_shardline_http_cache_frontend() {
    if bazel_program().is_none() {
        return;
    }

    let result = exercise_native_bazel_http_cache_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native bazel remote cache e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_bazel_remote_cache_toplevel_download_flow_works_against_shardline_http_cache_frontend()
 {
    if bazel_program().is_none() {
        return;
    }

    let result = exercise_native_bazel_http_cache_toplevel_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native bazel toplevel remote cache e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_push_pull_and_tag_listing_work_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_registry_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native oci registry e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_digest_reference_and_multi_tag_flow_work_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_digest_and_multi_tag_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native oci digest/multi-tag e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_registry_to_registry_copy_works_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_registry_to_registry_copy_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native oci registry-to-registry copy e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_multiarch_index_flow_works_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_multiarch_index_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native oci multiarch index e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_docker_schema2_flow_works_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_docker_schema2_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native oci docker schema2 e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_skopeo_creds_token_service_flow_works_against_shardline_oci_frontend() {
    if !(command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_oci_token_service_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native oci token-service e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_helm_oci_chart_push_and_pull_work_against_shardline_oci_frontend() {
    if !command_available("helm") {
        return;
    }

    let result = exercise_native_helm_oci_chart_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native helm oci e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_podman_pull_and_push_work_against_shardline_oci_frontend() {
    if !(command_available("podman") && command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_podman_oci_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native podman oci e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_docker_pull_and_push_work_against_shardline_oci_frontend() {
    if !(command_available("docker") && command_available("skopeo") && command_available("tar")) {
        return;
    }

    let result = exercise_native_docker_oci_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native docker oci e2e failed: {error:?}");
}

async fn exercise_native_git_lfs_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Lfs]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let remote_root = tempfile::tempdir()?;
    let worktree = tempfile::tempdir()?;
    let clone = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    let remote_repo_path = remote_root.path().join("origin.git");
    let remote_repo_path_string = remote_repo_path.to_string_lossy().to_string();
    let original_bytes = seeded_bytes(256 * 1024, 41);
    let original_path = worktree.path().join("asset.bin");

    run_command_checked(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(&remote_repo_path),
        "git init --bare",
    )?;

    run_git_with_home(worktree.path(), git_home.path(), &["init"])?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["config", "user.name", "Shardline Test"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["config", "user.email", "shardline@example.invalid"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["remote", "add", "origin", &remote_repo_path_string],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["install", "--local"],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["track", "*.bin"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &[
            "config",
            "lfs.url",
            &format!("{}/v1/lfs", runtime.base_url()),
        ],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["config", "lfs.locksverify", "false"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &[
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {write_token}"),
        ],
    )?;

    write_file(&original_path, &original_bytes)?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["add", ".gitattributes", "asset.bin"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["commit", "-m", "add lfs asset"],
    )?;
    run_git_with_home(worktree.path(), git_home.path(), &["branch", "-M", "main"])?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["push", "-u", "origin", "main"],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["push", "origin", "main"],
    )?;

    let mut clone_command = Command::new("git");
    prepare_command(&mut clone_command, git_home.path())
        .env("GIT_LFS_SKIP_SMUDGE", "1")
        .arg("clone")
        .arg("--branch")
        .arg("main")
        .arg(&remote_repo_path)
        .arg(clone.path());
    run_command_checked(&mut clone_command, "git clone")?;

    run_git_with_home(
        clone.path(),
        git_home.path(),
        &[
            "config",
            "lfs.url",
            &format!("{}/v1/lfs", runtime.base_url()),
        ],
    )?;
    run_git_with_home(
        clone.path(),
        git_home.path(),
        &["config", "lfs.locksverify", "false"],
    )?;
    run_git_with_home(
        clone.path(),
        git_home.path(),
        &[
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {read_token}"),
        ],
    )?;
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["install", "--local"],
    )?;
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[("GIT_ATTR_SOURCE", "HEAD")],
        "git-lfs",
        &["fetch", "origin", "main"],
    )?;
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[("GIT_ATTR_SOURCE", "HEAD")],
        "git-lfs",
        &["checkout", "asset.bin"],
    )?;

    let cloned_bytes = read(clone.path().join("asset.bin"))?;
    if cloned_bytes != original_bytes {
        return Err(
            ServerE2eInvariantError::new("git-lfs pulled bytes did not match source").into(),
        );
    }

    Ok(())
}

async fn exercise_native_git_lfs_pull_and_fetch_all_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Lfs]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let remote_root = tempfile::tempdir()?;
    let worktree = tempfile::tempdir()?;
    let clone = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    let remote_repo_path = remote_root.path().join("origin.git");
    let remote_repo_path_string = remote_repo_path.to_string_lossy().to_string();
    let original_bytes = seeded_bytes(128 * 1024, 77);

    run_command_checked(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(&remote_repo_path),
        "git init --bare",
    )?;

    run_git_with_home(worktree.path(), git_home.path(), &["init"])?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["config", "user.name", "Shardline Test"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["config", "user.email", "shardline@example.invalid"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["remote", "add", "origin", &remote_repo_path_string],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["install", "--local"],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["track", "*.bin"],
    )?;
    configure_lfs_client_repo(
        worktree.path(),
        git_home.path(),
        runtime.base_url(),
        &write_token,
    )?;

    write_file(worktree.path().join("asset.bin"), &original_bytes)?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["add", ".gitattributes", "asset.bin"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["commit", "-m", "add lfs asset"],
    )?;
    run_git_with_home(worktree.path(), git_home.path(), &["branch", "-M", "main"])?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        &["push", "-u", "origin", "main"],
    )?;
    run_git_with_home_and_env(
        worktree.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["push", "--all", "origin"],
    )?;

    let mut clone_command = Command::new("git");
    prepare_command(&mut clone_command, git_home.path())
        .env("GIT_LFS_SKIP_SMUDGE", "1")
        .arg("-c")
        .arg(format!("lfs.url={}/v1/lfs", runtime.base_url()))
        .arg("-c")
        .arg("lfs.locksverify=false")
        .arg("-c")
        .arg(format!(
            "http.extraHeader=Authorization: Bearer {read_token}"
        ))
        .arg("clone")
        .arg("--branch")
        .arg("main")
        .arg(&remote_repo_path)
        .arg(clone.path());
    run_command_checked(&mut clone_command, "git clone for lfs pull/fetch-all")?;

    configure_lfs_client_repo(
        clone.path(),
        git_home.path(),
        runtime.base_url(),
        &read_token,
    )?;
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[],
        "git-lfs",
        &["install", "--local"],
    )?;
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[("GIT_ATTR_SOURCE", "HEAD")],
        "git-lfs",
        &["pull", "origin"],
    )?;
    let cloned_bytes = read(clone.path().join("asset.bin"))?;
    if cloned_bytes != original_bytes {
        return Err(ServerE2eInvariantError::new("git-lfs pull bytes did not match source").into());
    }
    run_git_with_home_and_env(
        clone.path(),
        git_home.path(),
        &[("GIT_ATTR_SOURCE", "HEAD")],
        "git-lfs",
        &["fetch", "--all", "origin"],
    )?;
    let ls_files =
        run_git_with_home_and_env(clone.path(), git_home.path(), &[], "git-lfs", &["ls-files"])?;
    if !String::from_utf8_lossy(&ls_files.stdout).contains("asset.bin") {
        return Err(ServerE2eInvariantError::new("git-lfs ls-files did not list asset.bin").into());
    }

    Ok(())
}

async fn exercise_native_bazel_http_cache_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::BazelHttp]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let workspace = tempfile::tempdir()?;
    let bazel_home = tempfile::tempdir()?;
    let output_base_one = tempfile::tempdir()?;
    let output_base_two = tempfile::tempdir()?;
    let bazel = bazel_program().ok_or_else(|| {
        ServerE2eInvariantError::new("bazel or bazelisk is required for native bazel e2e")
    })?;

    write_file(
        workspace.path().join("MODULE.bazel"),
        "module(name = \"cachetest\")\n",
    )?;
    write_file(
        workspace.path().join("BUILD.bazel"),
        concat!(
            "genrule(\n",
            "    name = \"copy\",\n",
            "    srcs = [\"input.txt\"],\n",
            "    outs = [\"out.txt\"],\n",
            "    cmd = \"cat $(location input.txt) > $@\",\n",
            ")\n",
        ),
    )?;
    write_file(workspace.path().join("input.txt"), "hello bazel cache\n")?;

    let first_build = run_bazel_build(
        bazel,
        workspace.path(),
        bazel_home.path(),
        output_base_one.path(),
        runtime.base_url(),
        &write_token,
        true,
    )?;
    if !first_build.status.success() {
        return Err(command_error("initial bazel build", &first_build).into());
    }

    let built = read(workspace.path().join("bazel-bin").join("out.txt"))?;
    if built != b"hello bazel cache\n" {
        return Err(
            ServerE2eInvariantError::new("initial bazel build produced wrong bytes").into(),
        );
    }

    let second_build = run_bazel_build(
        bazel,
        workspace.path(),
        bazel_home.path(),
        output_base_two.path(),
        runtime.base_url(),
        &read_token,
        false,
    )?;
    if !second_build.status.success() {
        return Err(command_error("second bazel build", &second_build).into());
    }
    let combined_output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&second_build.stdout),
        String::from_utf8_lossy(&second_build.stderr)
    );
    if !combined_output.contains("remote cache hit") {
        return Err(ServerE2eInvariantError::new(format!(
            "expected bazel remote cache hit in output\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&second_build.stdout),
            String::from_utf8_lossy(&second_build.stderr)
        ))
        .into());
    }

    let rebuilt = read(workspace.path().join("bazel-bin").join("out.txt"))?;
    if rebuilt != b"hello bazel cache\n" {
        return Err(ServerE2eInvariantError::new("cached bazel build produced wrong bytes").into());
    }

    Ok(())
}

async fn exercise_native_bazel_http_cache_toplevel_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::BazelHttp]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let workspace = tempfile::tempdir()?;
    let bazel_home = tempfile::tempdir()?;
    let output_base_one = tempfile::tempdir()?;
    let output_base_two = tempfile::tempdir()?;
    let bazel = bazel_program().ok_or_else(|| {
        ServerE2eInvariantError::new("bazel or bazelisk is required for native bazel e2e")
    })?;

    write_file(
        workspace.path().join("MODULE.bazel"),
        "module(name = \"cachetest\")\n",
    )?;
    write_file(
        workspace.path().join("BUILD.bazel"),
        concat!(
            "genrule(\n",
            "    name = \"copy\",\n",
            "    srcs = [\"input.txt\"],\n",
            "    outs = [\"out.txt\"],\n",
            "    cmd = \"cat $(location input.txt) > $@\",\n",
            ")\n",
        ),
    )?;
    write_file(workspace.path().join("input.txt"), "hello bazel toplevel\n")?;

    let first_build = run_bazel_build_with_download_mode(
        bazel,
        workspace.path(),
        bazel_home.path(),
        output_base_one.path(),
        runtime.base_url(),
        &write_token,
        true,
        "toplevel",
    )?;
    if !first_build.status.success() {
        return Err(command_error("initial bazel toplevel build", &first_build).into());
    }

    let second_build = run_bazel_build_with_download_mode(
        bazel,
        workspace.path(),
        bazel_home.path(),
        output_base_two.path(),
        runtime.base_url(),
        &read_token,
        false,
        "toplevel",
    )?;
    if !second_build.status.success() {
        return Err(command_error("second bazel toplevel build", &second_build).into());
    }

    let combined_output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&second_build.stdout),
        String::from_utf8_lossy(&second_build.stderr)
    );
    if !combined_output.contains("remote cache hit") {
        return Err(ServerE2eInvariantError::new(format!(
            "expected bazel remote cache hit in toplevel mode\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&second_build.stdout),
            String::from_utf8_lossy(&second_build.stderr)
        ))
        .into());
    }
    let rebuilt = read(workspace.path().join("bazel-bin").join("out.txt"))?;
    if rebuilt != b"hello bazel toplevel\n" {
        return Err(
            ServerE2eInvariantError::new("toplevel bazel build produced wrong bytes").into(),
        );
    }

    Ok(())
}

async fn exercise_native_oci_registry_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let pulled_layout = tempfile::tempdir()?;
    let image_ref = format!("docker://{}/team/assets:v1", runtime.host_port());
    let repository_ref = format!("docker://{}/team/assets", runtime.host_port());
    let _layout =
        create_oci_image_layout(source_layout.path(), "v1", "hello from shardline oci\n")?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&image_ref),
        "skopeo copy oci -> registry",
    )?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg(&repository_ref),
        "skopeo list-tags",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    if !tags["Tags"]
        .as_array()
        .is_some_and(|tags| tags.iter().any(|tag| tag == "v1"))
    {
        return Err(ServerE2eInvariantError::new("oci tag listing did not include v1").into());
    }

    let manifest = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg("--raw")
            .arg(&image_ref),
        "skopeo inspect --raw",
    )?;
    let manifest = serde_json::from_slice::<Value>(&manifest.stdout)?;
    if manifest["config"]["digest"].as_str().is_none() {
        return Err(
            ServerE2eInvariantError::new("oci manifest was missing a config digest").into(),
        );
    }
    if !manifest["layers"]
        .as_array()
        .is_some_and(|layers| !layers.is_empty())
    {
        return Err(ServerE2eInvariantError::new("oci manifest did not contain any layers").into());
    }

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--src-tls-verify=false")
            .arg("--src-registry-token")
            .arg(&read_token)
            .arg(&image_ref)
            .arg(format!("oci:{}:mirror", pulled_layout.path().display())),
        "skopeo copy registry -> oci",
    )?;

    let pulled_index = read(pulled_layout.path().join("index.json"))?;
    let pulled_index = serde_json::from_slice::<Value>(&pulled_index)?;
    if !pulled_index["manifests"]
        .as_array()
        .is_some_and(|entries| !entries.is_empty())
    {
        return Err(ServerE2eInvariantError::new(
            "pulled oci layout did not contain an index manifest",
        )
        .into());
    }
    let pulled_blobs = std::fs::read_dir(pulled_layout.path().join("blobs").join("sha256"))?
        .filter_map(Result::ok)
        .count();
    if pulled_blobs == 0 {
        return Err(
            ServerE2eInvariantError::new("pulled oci layout did not contain any blobs").into(),
        );
    }

    Ok(())
}

async fn exercise_native_oci_digest_and_multi_tag_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let digest_layout = tempfile::tempdir()?;
    let repository_ref = format!("docker://{}/team/assets", runtime.host_port());
    let v1_ref = format!("{repository_ref}:v1");
    let stable_ref = format!("{repository_ref}:stable");

    let _layout =
        create_oci_image_layout(source_layout.path(), "v1", "hello from shardline oci\n")?;
    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&v1_ref),
        "skopeo copy oci -> registry v1",
    )?;
    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&stable_ref),
        "skopeo copy oci -> registry stable",
    )?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg(&repository_ref),
        "skopeo list-tags multi-tag",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    let tags = tags["Tags"].as_array().ok_or_else(|| {
        ServerE2eInvariantError::new("oci tag listing did not contain a tag array")
    })?;
    if !(tags.iter().any(|tag| tag == "v1") && tags.iter().any(|tag| tag == "stable")) {
        return Err(ServerE2eInvariantError::new(
            "oci tag listing did not include both v1 and stable",
        )
        .into());
    }

    let inspect = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg(&v1_ref),
        "skopeo inspect digest",
    )?;
    let inspect = serde_json::from_slice::<Value>(&inspect.stdout)?;
    let digest = inspect["Digest"]
        .as_str()
        .ok_or_else(|| ServerE2eInvariantError::new("skopeo inspect did not return a digest"))?;

    let config = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg("--config")
            .arg(&stable_ref),
        "skopeo inspect --config",
    )?;
    let config = serde_json::from_slice::<Value>(&config.stdout)?;
    if config["architecture"].as_str().is_none() || config["rootfs"].is_null() {
        return Err(ServerE2eInvariantError::new(
            "skopeo inspect --config returned an unexpected payload",
        )
        .into());
    }

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--src-tls-verify=false")
            .arg("--src-registry-token")
            .arg(&read_token)
            .arg(format!("{repository_ref}@{digest}"))
            .arg(format!("oci:{}:bydigest", digest_layout.path().display())),
        "skopeo copy by digest",
    )?;
    let digest_index = read(digest_layout.path().join("index.json"))?;
    let digest_index = serde_json::from_slice::<Value>(&digest_index)?;
    if !digest_index["manifests"]
        .as_array()
        .is_some_and(|entries| !entries.is_empty())
    {
        return Err(ServerE2eInvariantError::new(
            "digest-addressed skopeo copy did not produce an OCI index",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_oci_registry_to_registry_copy_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let source_repo = format!("docker://{}/team/assets/source", runtime.host_port());
    let source_ref = format!("{source_repo}:v1");
    let target_repo = format!("docker://{}/team/assets/mirror", runtime.host_port());
    let target_ref = format!("{target_repo}:v1");

    let _layout =
        create_oci_image_layout(source_layout.path(), "v1", "registry to registry copy\n")?;
    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&source_ref),
        "skopeo seed registry source",
    )?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--src-tls-verify=false")
            .arg("--src-registry-token")
            .arg(&read_token)
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(&source_ref)
            .arg(&target_ref),
        "skopeo copy registry -> registry",
    )?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg(&target_repo),
        "skopeo list-tags registry mirror",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    if !tags["Tags"]
        .as_array()
        .is_some_and(|tags| tags.iter().any(|tag| tag == "v1"))
    {
        return Err(ServerE2eInvariantError::new(
            "oci registry mirror tag listing did not include v1",
        )
        .into());
    }

    let manifest = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg("--raw")
            .arg(&target_ref),
        "skopeo inspect --raw registry mirror",
    )?;
    let manifest = serde_json::from_slice::<Value>(&manifest.stdout)?;
    if manifest["config"]["digest"].as_str().is_none() {
        return Err(ServerE2eInvariantError::new(
            "oci registry mirror manifest was missing a config digest",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_oci_multiarch_index_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let pulled_layout = tempfile::tempdir()?;
    let image_ref = format!("docker://{}/team/assets:multi", runtime.host_port());

    create_oci_multiarch_layout(source_layout.path(), "multi")?;
    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--all")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:multi", source_layout.path().display()))
            .arg(&image_ref),
        "skopeo copy multiarch oci -> registry",
    )?;

    let raw = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg("--raw")
            .arg(&image_ref),
        "skopeo inspect --raw multiarch",
    )?;
    let raw = serde_json::from_slice::<Value>(&raw.stdout)?;
    if raw["mediaType"] != "application/vnd.oci.image.index.v1+json" {
        return Err(ServerE2eInvariantError::new(
            "multiarch registry payload was not an OCI image index",
        )
        .into());
    }
    if !raw["manifests"]
        .as_array()
        .is_some_and(|entries| entries.len() == 2)
    {
        return Err(ServerE2eInvariantError::new(
            "multiarch registry payload did not contain two manifests",
        )
        .into());
    }

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--all")
            .arg("--src-tls-verify=false")
            .arg("--src-registry-token")
            .arg(&read_token)
            .arg(&image_ref)
            .arg(format!("oci:{}:mirror", pulled_layout.path().display())),
        "skopeo copy multiarch registry -> oci",
    )?;
    let pulled_index = read(pulled_layout.path().join("index.json"))?;
    let pulled_index = serde_json::from_slice::<Value>(&pulled_index)?;
    if !pulled_index["manifests"]
        .as_array()
        .is_some_and(|entries| entries.len() == 1)
    {
        return Err(ServerE2eInvariantError::new(
            "pulled multiarch OCI layout did not expose a named index entry",
        )
        .into());
    }
    let pulled_blobs = std::fs::read_dir(pulled_layout.path().join("blobs").join("sha256"))?
        .filter_map(Result::ok)
        .count();
    if pulled_blobs < 5 {
        return Err(ServerE2eInvariantError::new(
            "pulled multiarch OCI layout did not contain enough blobs",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_oci_docker_schema2_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let image_ref = format!("docker://{}/team/assets:v2s2", runtime.host_port());
    let _layout =
        create_oci_image_layout(source_layout.path(), "v1", "docker schema2 compatibility\n")?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--format")
            .arg("v2s2")
            .arg("--dest-tls-verify=false")
            .arg("--dest-registry-token")
            .arg(&write_token)
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&image_ref),
        "skopeo copy docker schema2 oci -> registry",
    )?;

    let raw = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--registry-token")
            .arg(&read_token)
            .arg("--raw")
            .arg(&image_ref),
        "skopeo inspect --raw docker schema2",
    )?;
    let raw = serde_json::from_slice::<Value>(&raw.stdout)?;
    if raw["schemaVersion"].as_u64() != Some(2) {
        return Err(ServerE2eInvariantError::new(
            "docker schema2 payload did not expose schemaVersion 2",
        )
        .into());
    }
    if raw["config"]["digest"].as_str().is_none() {
        return Err(ServerE2eInvariantError::new(
            "docker schema2 payload was missing a config digest",
        )
        .into());
    }

    let manifest = Client::new()
        .get(format!(
            "{}/v2/team/assets/manifests/v2s2",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    if manifest.status() != reqwest::StatusCode::OK {
        return Err(ServerE2eInvariantError::new("docker schema2 manifest fetch failed").into());
    }
    if manifest
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        != Some("application/vnd.docker.distribution.manifest.v2+json")
    {
        return Err(ServerE2eInvariantError::new(
            "docker schema2 manifest did not preserve the docker media type",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_oci_token_service_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let image_ref = format!("docker://{}/team/assets:creds", runtime.host_port());
    let repository_ref = format!("docker://{}/team/assets", runtime.host_port());
    let _layout = create_oci_image_layout(source_layout.path(), "v1", "oci creds flow\n")?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-creds")
            .arg(format!("stexs:{write_token}"))
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(&image_ref),
        "skopeo copy via creds token service",
    )?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--creds")
            .arg(format!("stexs:{read_token}"))
            .arg(&repository_ref),
        "skopeo list-tags via creds token service",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    if !tags["Tags"]
        .as_array()
        .is_some_and(|tags| tags.iter().any(|tag| tag == "creds"))
    {
        return Err(ServerE2eInvariantError::new(
            "oci token-service tag listing did not include creds",
        )
        .into());
    }

    let manifest = run_command_checked(
        Command::new("skopeo")
            .arg("inspect")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--creds")
            .arg(format!("stexs:{read_token}"))
            .arg("--raw")
            .arg(&image_ref),
        "skopeo inspect --raw via creds token service",
    )?;
    let manifest = serde_json::from_slice::<Value>(&manifest.stdout)?;
    if manifest["config"]["digest"].as_str().is_none() {
        return Err(ServerE2eInvariantError::new(
            "oci token-service manifest was missing a config digest",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_helm_oci_chart_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let workdir = tempfile::tempdir()?;
    let helm_home = tempfile::tempdir()?;
    let package_dir = workdir.path().join("package");
    let pull_dir = workdir.path().join("pull");
    let chart_dir = workdir.path().join("chart");
    create_dir_all(&package_dir)?;
    create_dir_all(&pull_dir)?;
    create_dir_all(chart_dir.join("templates"))?;

    write_file(
        chart_dir.join("Chart.yaml"),
        r#"apiVersion: v2
name: stexs-chart
description: Shardline helm chart e2e
type: application
version: 0.1.0
appVersion: "1.0.0"
"#,
    )?;
    write_file(
        chart_dir.join("values.yaml"),
        "replicaCount: 1\nimage:\n  repository: stexs/example\n  tag: latest\n",
    )?;
    write_file(
        chart_dir.join("templates").join("configmap.yaml"),
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Chart.Name }}
data:
  replicaCount: "{{ .Values.replicaCount }}"
"#,
    )?;

    let mut package_command = Command::new("helm");
    prepare_helm_command(&mut package_command, helm_home.path())
        .arg("package")
        .arg(&chart_dir)
        .arg("--destination")
        .arg(&package_dir);
    run_command_checked(&mut package_command, "helm package")?;

    let chart_package = package_dir.join("stexs-chart-0.1.0.tgz");
    if !chart_package.exists() {
        return Err(
            ServerE2eInvariantError::new("helm package did not produce a chart tarball").into(),
        );
    }

    let mut push_command = Command::new("helm");
    prepare_helm_command(&mut push_command, helm_home.path())
        .arg("push")
        .arg(&chart_package)
        .arg(format!("oci://{}/team/assets/charts", runtime.host_port()))
        .arg("--plain-http")
        .arg("--username")
        .arg("stexs")
        .arg("--password")
        .arg(&write_token);
    run_command_checked(&mut push_command, "helm push oci chart")?;

    let mut pull_command = Command::new("helm");
    prepare_helm_command(&mut pull_command, helm_home.path())
        .arg("pull")
        .arg(format!(
            "oci://{}/team/assets/charts/stexs-chart",
            runtime.host_port()
        ))
        .arg("--version")
        .arg("0.1.0")
        .arg("--plain-http")
        .arg("--username")
        .arg("stexs")
        .arg("--password")
        .arg(&read_token)
        .arg("--destination")
        .arg(&pull_dir);
    run_command_checked(&mut pull_command, "helm pull oci chart")?;

    let pulled_chart = pull_dir.join("stexs-chart-0.1.0.tgz");
    if !pulled_chart.exists() {
        return Err(
            ServerE2eInvariantError::new("helm pull did not produce a chart tarball").into(),
        );
    }

    let mut show_command = Command::new("helm");
    prepare_helm_command(&mut show_command, helm_home.path())
        .arg("show")
        .arg("chart")
        .arg(format!(
            "oci://{}/team/assets/charts/stexs-chart",
            runtime.host_port()
        ))
        .arg("--version")
        .arg("0.1.0")
        .arg("--plain-http")
        .arg("--username")
        .arg("stexs")
        .arg("--password")
        .arg(&read_token);
    let chart_metadata = run_command_checked(&mut show_command, "helm show chart")?;
    let chart_metadata = String::from_utf8(chart_metadata.stdout)?;
    if !chart_metadata.contains("name: stexs-chart") || !chart_metadata.contains("version: 0.1.0") {
        return Err(
            ServerE2eInvariantError::new("helm show chart returned unexpected metadata").into(),
        );
    }

    Ok(())
}

async fn exercise_native_podman_oci_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let client_home = tempfile::tempdir()?;
    let runtime_dir = client_home.path().join("run");
    let auth_file = client_home.path().join("auth.json");
    create_dir_all(&runtime_dir)?;

    let seeded_ref = format!("{}/team/assets:podman-seed", runtime.host_port());
    let copied_ref = format!("{}/team/assets:podman-copy", runtime.host_port());
    let _layout = create_oci_image_layout(source_layout.path(), "v1", "podman registry flow\n")?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-creds")
            .arg(format!("stexs:{write_token}"))
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(format!("docker://{seeded_ref}")),
        "skopeo seed podman registry image",
    )?;

    let mut login = Command::new("podman");
    prepare_podman_command(&mut login, client_home.path(), &runtime_dir, &auth_file)
        .arg("login")
        .arg("--tls-verify=false")
        .arg("--username")
        .arg("stexs")
        .arg("--password-stdin")
        .arg(runtime.host_port());
    run_command_with_stdin_checked(&mut login, read_token.as_bytes(), "podman login")?;

    let mut pull = Command::new("podman");
    prepare_podman_command(&mut pull, client_home.path(), &runtime_dir, &auth_file)
        .arg("pull")
        .arg("--tls-verify=false")
        .arg(&seeded_ref);
    run_command_checked(&mut pull, "podman pull")?;

    let mut tag = Command::new("podman");
    prepare_podman_command(&mut tag, client_home.path(), &runtime_dir, &auth_file)
        .arg("tag")
        .arg(&seeded_ref)
        .arg(&copied_ref);
    run_command_checked(&mut tag, "podman tag")?;

    let mut push = Command::new("podman");
    prepare_podman_command(&mut push, client_home.path(), &runtime_dir, &auth_file)
        .arg("push")
        .arg("--tls-verify=false")
        .arg("--creds")
        .arg(format!("stexs:{write_token}"))
        .arg(&copied_ref);
    run_command_checked(&mut push, "podman push")?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--creds")
            .arg(format!("stexs:{read_token}"))
            .arg(format!("docker://{}/team/assets", runtime.host_port())),
        "skopeo list-tags after podman push",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    if !tags["Tags"]
        .as_array()
        .is_some_and(|tags| tags.iter().any(|tag| tag == "podman-copy"))
    {
        return Err(ServerE2eInvariantError::new(
            "podman push did not create the expected registry tag",
        )
        .into());
    }

    Ok(())
}

async fn exercise_native_docker_oci_flow() -> Result<(), TestError> {
    let runtime = start_runtime(&[ServerFrontend::Oci]).await?;
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let source_layout = tempfile::tempdir()?;
    let docker_home = tempfile::tempdir()?;
    let docker_config = docker_home.path().join(".docker");
    create_dir_all(&docker_config)?;

    let seeded_ref = format!("{}/team/assets:docker-seed", runtime.host_port());
    let copied_ref = format!("{}/team/assets:docker-copy", runtime.host_port());
    let _layout = create_oci_image_layout(source_layout.path(), "v1", "docker registry flow\n")?;

    run_command_checked(
        Command::new("skopeo")
            .arg("copy")
            .arg("--retry-times")
            .arg("1")
            .arg("--dest-tls-verify=false")
            .arg("--dest-creds")
            .arg(format!("stexs:{write_token}"))
            .arg(format!("oci:{}:v1", source_layout.path().display()))
            .arg(format!("docker://{seeded_ref}")),
        "skopeo seed docker registry image",
    )?;

    let mut login = Command::new("docker");
    prepare_docker_command(&mut login, docker_home.path(), &docker_config)
        .arg("login")
        .arg("--username")
        .arg("stexs")
        .arg("--password-stdin")
        .arg(runtime.host_port());
    run_command_with_stdin_checked(&mut login, write_token.as_bytes(), "docker login")?;

    let mut pull = Command::new("docker");
    prepare_docker_command(&mut pull, docker_home.path(), &docker_config)
        .arg("pull")
        .arg(&seeded_ref);
    run_command_checked(&mut pull, "docker pull")?;

    let mut tag = Command::new("docker");
    prepare_docker_command(&mut tag, docker_home.path(), &docker_config)
        .arg("tag")
        .arg(&seeded_ref)
        .arg(&copied_ref);
    run_command_checked(&mut tag, "docker tag")?;

    let mut push = Command::new("docker");
    prepare_docker_command(&mut push, docker_home.path(), &docker_config)
        .arg("push")
        .arg(&copied_ref);
    run_command_checked(&mut push, "docker push")?;

    let tags = run_command_checked(
        Command::new("skopeo")
            .arg("list-tags")
            .arg("--retry-times")
            .arg("1")
            .arg("--tls-verify=false")
            .arg("--creds")
            .arg(format!("stexs:{read_token}"))
            .arg(format!("docker://{}/team/assets", runtime.host_port())),
        "skopeo list-tags after docker push",
    )?;
    let tags = serde_json::from_slice::<Value>(&tags.stdout)?;
    if !tags["Tags"]
        .as_array()
        .is_some_and(|tags| tags.iter().any(|tag| tag == "docker-copy"))
    {
        return Err(ServerE2eInvariantError::new(
            "docker push did not create the expected registry tag",
        )
        .into());
    }

    let mut cleanup_seed = Command::new("docker");
    let _cleanup_seed = run_command(
        prepare_docker_command(&mut cleanup_seed, docker_home.path(), &docker_config)
            .arg("image")
            .arg("rm")
            .arg("-f")
            .arg(&seeded_ref),
    );
    let mut cleanup_copy = Command::new("docker");
    let _cleanup_copy = run_command(
        prepare_docker_command(&mut cleanup_copy, docker_home.path(), &docker_config)
            .arg("image")
            .arg("rm")
            .arg("-f")
            .arg(&copied_ref),
    );

    Ok(())
}

async fn start_runtime(frontends: &[ServerFrontend]) -> Result<FrontendRuntime, TestError> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(FrontendRuntime {
        _storage: storage,
        base_url,
        host_port: addr.to_string(),
        server,
    })
}

fn scoped_token(scope: TokenScope, owner: &str, repo: &str) -> Result<String, TestError> {
    let signer = TokenSigner::new(b"signing-key")?;
    let repository = RepositoryScope::new(RepositoryProvider::GitHub, owner, repo, Some("main"))?;
    let claims = TokenClaims::new("local", "native-client", scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

async fn wait_for_health(base_url: &str) -> Result<(), TestError> {
    let client = Client::new();
    for _attempt in 0..50 {
        let response = client.get(format!("{base_url}/healthz")).send().await;
        if let Ok(response) = response
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(ServerE2eInvariantError::new("server did not become healthy").into())
}

fn command_available(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn bazel_program() -> Option<&'static str> {
    if command_available("bazel") {
        return Some("bazel");
    }
    if command_available("bazelisk") {
        return Some("bazelisk");
    }

    None
}

fn prepare_helm_command<'command>(
    command: &'command mut Command,
    home: &Path,
) -> &'command mut Command {
    let config_home = home.join(".config");
    let cache_home = home.join(".cache");
    let data_home = home.join(".local").join("share");
    command
        .env("HOME", home)
        .env("HELM_CONFIG_HOME", &config_home)
        .env("HELM_CACHE_HOME", &cache_home)
        .env("HELM_DATA_HOME", &data_home)
        .env("XDG_CONFIG_HOME", &config_home)
        .env("XDG_CACHE_HOME", &cache_home)
        .env("XDG_DATA_HOME", &data_home)
}

fn prepare_podman_command<'command>(
    command: &'command mut Command,
    home: &Path,
    runtime_dir: &Path,
    auth_file: &Path,
) -> &'command mut Command {
    command
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime_dir)
        .env("REGISTRY_AUTH_FILE", auth_file)
}

fn prepare_docker_command<'command>(
    command: &'command mut Command,
    home: &Path,
    docker_config: &Path,
) -> &'command mut Command {
    command
        .env("HOME", home)
        .env("DOCKER_CONFIG", docker_config)
}

fn prepare_command<'command>(command: &'command mut Command, home: &Path) -> &'command mut Command {
    command
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "true")
}

fn run_git_with_home(repo: &Path, home: &Path, args: &[&str]) -> Result<Output, TestError> {
    let mut command = Command::new("git");
    prepare_command(&mut command, home)
        .current_dir(repo)
        .args(args);
    let output = run_command(&mut command)?;
    if output.status.success() {
        return Ok(output);
    }

    Err(command_error("git command", &output).into())
}

fn run_git_with_home_and_env(
    repo: &Path,
    home: &Path,
    envs: &[(&str, &str)],
    program: &str,
    args: &[&str],
) -> Result<Output, TestError> {
    let mut command = Command::new(program);
    prepare_command(&mut command, home)
        .current_dir(repo)
        .args(args);
    for &(key, value) in envs {
        command.env(key, value);
    }
    let output = run_command(&mut command)?;
    if output.status.success() {
        return Ok(output);
    }

    Err(command_error(program, &output).into())
}

fn configure_lfs_client_repo(
    repo: &Path,
    home: &Path,
    base_url: &str,
    token: &str,
) -> Result<(), TestError> {
    run_git_with_home(
        repo,
        home,
        &["config", "lfs.url", &format!("{base_url}/v1/lfs")],
    )?;
    run_git_with_home(repo, home, &["config", "lfs.locksverify", "false"])?;
    run_git_with_home(
        repo,
        home,
        &[
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {token}"),
        ],
    )?;
    Ok(())
}

fn run_bazel_build(
    program: &str,
    workspace: &Path,
    home: &Path,
    output_base: &Path,
    base_url: &str,
    token: &str,
    allow_uploads: bool,
) -> Result<Output, TestError> {
    run_bazel_build_with_download_mode(
        program,
        workspace,
        home,
        output_base,
        base_url,
        token,
        allow_uploads,
        "all",
    )
}

fn run_bazel_build_with_download_mode(
    program: &str,
    workspace: &Path,
    home: &Path,
    output_base: &Path,
    base_url: &str,
    token: &str,
    allow_uploads: bool,
    download_mode: &str,
) -> Result<Output, TestError> {
    let mut command = Command::new(program);
    command
        .current_dir(workspace)
        .env("HOME", home)
        .arg("--batch")
        .arg("--bazelrc=/dev/null")
        .arg("--nohome_rc")
        .arg("--nosystem_rc")
        .arg("--noworkspace_rc")
        .arg(format!("--output_base={}", output_base.display()))
        .arg("build")
        .arg("//:copy")
        .arg("--show_result=0")
        .arg("--noshow_progress")
        .arg("--color=no")
        .arg("--curses=no")
        .arg(format!("--remote_download_outputs={download_mode}"))
        .arg(format!("--remote_cache={base_url}/v1/bazel/cache"))
        .arg(format!("--remote_header=Authorization=Bearer {token}"))
        .arg(format!(
            "--remote_cache_header=Authorization=Bearer {token}"
        ))
        .arg(format!(
            "--remote_upload_local_results={}",
            if allow_uploads { "true" } else { "false" }
        ));
    let output = run_command(&mut command)?;
    Ok(output)
}

fn run_command(command: &mut Command) -> Result<Output, IoError> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
}

fn run_command_with_stdin(command: &mut Command, input: &[u8]) -> Result<Output, IoError> {
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(input)?;
        stdin.write_all(b"\n")?;
    }
    child.wait_with_output()
}

fn run_command_checked(command: &mut Command, description: &str) -> Result<Output, TestError> {
    let output = run_command(command)?;
    if output.status.success() {
        return Ok(output);
    }

    Err(command_error(description, &output).into())
}

fn run_command_with_stdin_checked(
    command: &mut Command,
    input: &[u8],
    description: &str,
) -> Result<Output, TestError> {
    let output = run_command_with_stdin(command, input)?;
    if output.status.success() {
        return Ok(output);
    }

    Err(command_error(description, &output).into())
}

fn command_error(description: &str, output: &Output) -> IoError {
    ServerE2eInvariantError::new(format!(
        "{description} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
    .into()
}

fn seeded_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

struct OciLayoutSummary {
    _config_digest: String,
    _layer_digest: String,
}

fn create_oci_image_layout(
    root: &Path,
    tag: &str,
    payload: &str,
) -> Result<OciLayoutSummary, TestError> {
    let blobs_root = root.join("blobs").join("sha256");
    let layer_root = root.join("layer-root");
    create_dir_all(&blobs_root)?;
    create_dir_all(&layer_root)?;
    write_file(
        root.join("oci-layout"),
        to_vec(&json!({ "imageLayoutVersion": "1.0.0" }))?,
    )?;
    write_file(layer_root.join("payload.txt"), payload)?;

    run_command_checked(
        Command::new("tar")
            .arg("-C")
            .arg(&layer_root)
            .arg("-cf")
            .arg(root.join("layer.tar"))
            .arg("."),
        "tar create layer",
    )?;
    let layer_bytes = read(root.join("layer.tar"))?;
    let layer_digest = hex::encode(Sha256::digest(&layer_bytes));
    write_file(blobs_root.join(&layer_digest), &layer_bytes)?;

    let config_bytes = to_vec(&json!({
        "architecture": "amd64",
        "os": "linux",
        "rootfs": {
            "type": "layers",
            "diff_ids": [format!("sha256:{layer_digest}")],
        },
        "config": {},
    }))?;
    let config_digest = hex::encode(Sha256::digest(&config_bytes));
    write_file(blobs_root.join(&config_digest), &config_bytes)?;

    let manifest_bytes = to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{config_digest}"),
            "size": config_bytes.len(),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": format!("sha256:{layer_digest}"),
            "size": layer_bytes.len(),
        }],
    }))?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    write_file(blobs_root.join(&manifest_digest), &manifest_bytes)?;

    let index_bytes = to_vec(&json!({
        "schemaVersion": 2,
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": format!("sha256:{manifest_digest}"),
            "size": manifest_bytes.len(),
            "annotations": {
                "org.opencontainers.image.ref.name": tag,
            },
        }],
    }))?;
    write_file(root.join("index.json"), &index_bytes)?;

    Ok(OciLayoutSummary {
        _config_digest: config_digest,
        _layer_digest: layer_digest,
    })
}

fn create_oci_multiarch_layout(root: &Path, tag: &str) -> Result<(), TestError> {
    let blobs_root = root.join("blobs").join("sha256");
    create_dir_all(&blobs_root)?;
    write_file(
        root.join("oci-layout"),
        to_vec(&json!({ "imageLayoutVersion": "1.0.0" }))?,
    )?;

    let amd64 = create_oci_manifest_blob(root, "amd64", "linux", "hello amd64\n")?;
    let arm64 = create_oci_manifest_blob(root, "arm64", "linux", "hello arm64\n")?;
    let image_index = to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": format!("sha256:{}", amd64.manifest_digest),
                "size": amd64.manifest_size,
                "platform": {
                    "architecture": "amd64",
                    "os": "linux",
                },
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": format!("sha256:{}", arm64.manifest_digest),
                "size": arm64.manifest_size,
                "platform": {
                    "architecture": "arm64",
                    "os": "linux",
                },
            }
        ],
    }))?;
    let image_index_digest = hex::encode(Sha256::digest(&image_index));
    write_file(blobs_root.join(&image_index_digest), &image_index)?;

    let index_bytes = to_vec(&json!({
        "schemaVersion": 2,
        "manifests": [{
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "digest": format!("sha256:{image_index_digest}"),
            "size": image_index.len(),
            "annotations": {
                "org.opencontainers.image.ref.name": tag,
            },
        }],
    }))?;
    write_file(root.join("index.json"), &index_bytes)?;
    Ok(())
}

struct OciManifestBlobSummary {
    manifest_digest: String,
    manifest_size: usize,
}

fn create_oci_manifest_blob(
    root: &Path,
    architecture: &str,
    os: &str,
    payload: &str,
) -> Result<OciManifestBlobSummary, TestError> {
    let blobs_root = root.join("blobs").join("sha256");
    let layer_root = root.join(format!("layer-root-{architecture}"));
    create_dir_all(&blobs_root)?;
    create_dir_all(&layer_root)?;
    write_file(layer_root.join("payload.txt"), payload)?;

    let layer_tar_path = root.join(format!("layer-{architecture}.tar"));
    run_command_checked(
        Command::new("tar")
            .arg("-C")
            .arg(&layer_root)
            .arg("-cf")
            .arg(&layer_tar_path)
            .arg("."),
        "tar create multiarch layer",
    )?;
    let layer_bytes = read(&layer_tar_path)?;
    let layer_digest = hex::encode(Sha256::digest(&layer_bytes));
    write_file(blobs_root.join(&layer_digest), &layer_bytes)?;

    let config_bytes = to_vec(&json!({
        "architecture": architecture,
        "os": os,
        "rootfs": {
            "type": "layers",
            "diff_ids": [format!("sha256:{layer_digest}")],
        },
        "config": {},
    }))?;
    let config_digest = hex::encode(Sha256::digest(&config_bytes));
    write_file(blobs_root.join(&config_digest), &config_bytes)?;

    let manifest_bytes = to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{config_digest}"),
            "size": config_bytes.len(),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": format!("sha256:{layer_digest}"),
            "size": layer_bytes.len(),
        }],
    }))?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    write_file(blobs_root.join(&manifest_digest), &manifest_bytes)?;

    Ok(OciManifestBlobSummary {
        manifest_digest,
        manifest_size: manifest_bytes.len(),
    })
}
