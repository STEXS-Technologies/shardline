mod support;

use std::{
    error::Error,
    fs::{create_dir_all, read, write as write_file},
    io::{Cursor, Error as IoError, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    str::from_utf8,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri,
        header::{CONTENT_LENGTH, CONTENT_TYPE, HOST},
    },
    routing::{any, get, post},
    serve as serve_http,
};
use reqwest::{Client, header::RANGE};
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_slice, json, to_vec};
use sha2::{Digest, Sha256};
use shardline_index::{
    FileChunkRecord, FileRecord, LocalRecordStore, RecordStore, RepositoryRecordScope,
    parse_xet_hash_hex,
};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    FileReconstructionResponse, ProviderTokenIssueRequest, ProviderTokenIssueResponse,
    ServerConfig, ServerStatsResponse, serve_with_listener, try_for_each_serialized_xorb_chunk,
    validate_serialized_xorb,
};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, task::spawn_blocking, time::sleep};

type TestError = Box<dyn Error + Send + Sync>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_xet_push_uploads_lfs_tracked_content_through_shardline_native_cas() {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return;
    }

    let result = exercise_git_xet_push_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "git-xet push e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_xet_clone_fetch_pull_and_checkout_flow_uses_native_cas() {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return;
    }

    let result = exercise_git_xet_clone_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "git-xet clone e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_xet_push_reuses_chunks_across_repositories_and_keeps_reads_scoped() {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return;
    }

    let result = exercise_git_xet_multi_repo_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "git-xet multi-repo e2e failed: {error:?}");
}

async fn exercise_git_xet_push_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let forge_root = tempfile::tempdir()?;
    let worktree = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    let provider_config = write_generic_provider_config(storage.path())?;
    let git_project_root = forge_root.path().join("repos");
    let remote_repo_path = git_project_root.join("origin.git");
    create_dir_all(&git_project_root)?;
    create_dir_all(git_home.path().join(".config"))?;

    let shardline_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let shardline_addr = shardline_listener.local_addr()?;
    let shardline_base_url = format!("http://{shardline_addr}");
    let shardline_config = ServerConfig::new(
        shardline_addr,
        shardline_base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "generic-bridge".to_owned(),
        NonZeroU64::new(300).ok_or("token ttl")?,
    )?;
    let shardline_server =
        spawn(async move { serve_with_listener(shardline_config, shardline_listener).await });

    let client = Client::new();
    wait_for_health(&client, &shardline_base_url).await?;

    let cas_proxy_state = CasProxyState {
        upstream_base_url: shardline_base_url.clone(),
        observations: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
    };
    let cas_proxy_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let cas_proxy_addr = cas_proxy_listener.local_addr()?;
    let cas_proxy_base_url = format!("http://{cas_proxy_addr}");
    let cas_proxy_router = Router::new()
        .fallback(any(handle_cas_proxy))
        .with_state(cas_proxy_state.clone());
    let cas_proxy_server =
        spawn(async move { serve_http(cas_proxy_listener, cas_proxy_router).await });

    let bridge_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bridge_addr = bridge_listener.local_addr()?;
    let bridge_state = ForgeState {
        shardline_api_base_url: shardline_base_url.clone(),
        cas_base_url: cas_proxy_base_url.clone(),
        bridge_base_url: format!("http://{bridge_addr}"),
        provider_key: "provider-bootstrap".to_owned(),
        owner: "team".to_owned(),
        repo: "assets".to_owned(),
        requests: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
        git_project_root: git_project_root.clone(),
        storage_root: storage.path().to_path_buf(),
    };
    let forge_url = format!("http://xet:xet@{bridge_addr}");
    let bridge_router = Router::new()
        .route("/origin.git/info/lfs/objects/batch", post(handle_lfs_batch))
        .route("/lfs/download/{oid}", get(handle_lfs_download))
        .fallback(any(handle_git_http))
        .with_state(bridge_state.clone());
    let bridge_server = spawn(async move { serve_http(bridge_listener, bridge_router).await });

    let init_remote = run_command(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(&remote_repo_path),
    )?;
    if !init_remote.status.success() {
        return Err(command_error("git init --bare", &init_remote).into());
    }
    run_command_checked(
        Command::new("git")
            .arg("--git-dir")
            .arg(&remote_repo_path)
            .args(["config", "http.receivepack", "true"]),
        "git config http.receivepack true",
    )?;

    let mut init_repo_command = Command::new("git");
    with_test_home(&mut init_repo_command, git_home.path())
        .current_dir(worktree.path())
        .arg("init");
    let init_repo = run_command(&mut init_repo_command)?;
    if !init_repo.status.success() {
        return Err(command_error("git init", &init_repo).into());
    }

    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["config", "user.name", "Shardline Test"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["config", "user.email", "shardline@example.invalid"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        [
            "config",
            "lfs.url",
            &format!("{forge_url}/origin.git/info/lfs"),
        ],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["config", "lfs.locksverify", "false"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["config", "lfs.concurrenttransfers", "1"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        [
            "remote",
            "add",
            "origin",
            &format!("{forge_url}/origin.git"),
        ],
    )?;

    let mut git_lfs_install = Command::new("git-lfs");
    with_test_home(&mut git_lfs_install, git_home.path())
        .current_dir(worktree.path())
        .args(["install", "--local"]);
    run_command_checked(&mut git_lfs_install, "git-lfs install --local")?;
    let mut git_xet_install = Command::new("git-xet");
    with_test_home(&mut git_xet_install, git_home.path())
        .current_dir(worktree.path())
        .args([
            "install",
            "--local",
            "--path",
            worktree.path().to_str().ok_or("worktree path utf-8")?,
        ]);
    run_command_checked(&mut git_xet_install, "git-xet install --local")?;
    let mut git_xet_track = Command::new("git-xet");
    with_test_home(&mut git_xet_track, git_home.path())
        .current_dir(worktree.path())
        .args(["track", "*.bin"]);
    run_command_checked(&mut git_xet_track, "git-xet track")?;

    let file_path = worktree.path().join("asset.bin");
    write_file(&file_path, seeded_bytes(512 * 1024, 17))?;

    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["add", ".gitattributes", "asset.bin"],
    )?;
    run_git_with_home(
        worktree.path(),
        git_home.path(),
        ["commit", "-m", "add xet tracked asset"],
    )?;
    run_git_with_home(worktree.path(), git_home.path(), ["branch", "-M", "main"])?;
    let mut push_command = Command::new("git");
    with_test_home(&mut push_command, git_home.path())
        .current_dir(worktree.path())
        .args(["push", "-u", "origin", "main"]);
    let push = run_command(&mut push_command)?;
    if !push.status.success() {
        return Err(ServerE2eInvariantError::new(format!(
            "{}\ncas observations:\n{}\nlfs requests:\n{}",
            command_error("git push", &push),
            format_cas_observations(&cas_proxy_state.observations)?,
            format_lfs_requests(&bridge_state.requests)?,
        ))
        .into());
    }

    let requests = bridge_state
        .requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("bridge request lock poisoned"))?
        .clone();
    if requests.is_empty() {
        return Err(
            ServerE2eInvariantError::new("lfs bridge did not receive any batch requests").into(),
        );
    }
    if !requests.iter().any(|request| request.operation == "upload") {
        return Err(ServerE2eInvariantError::new(
            "lfs bridge did not receive an upload batch request",
        )
        .into());
    }
    if !requests
        .iter()
        .filter(|request| request.operation == "upload")
        .all(|request| request.transfers.iter().any(|transfer| transfer == "xet"))
    {
        return Err(ServerE2eInvariantError::new(
            "git-lfs upload batch request did not advertise the xet transfer",
        )
        .into());
    }

    let read_token =
        issue_provider_token(&client, &shardline_base_url, TokenScopeRequest::Read).await?;
    let stats = client
        .get(format!("{shardline_base_url}/v1/stats"))
        .bearer_auth(&read_token.token)
        .send()
        .await?
        .error_for_status()?
        .json::<ServerStatsResponse>()
        .await?;
    if stats.chunks == 0 {
        return Err(ServerE2eInvariantError::new(
            "shardline stats reported zero stored chunks after git-xet push",
        )
        .into());
    }
    if stats.files == 0 {
        return Err(ServerE2eInvariantError::new(
            "shardline stats reported zero stored files after git-xet push",
        )
        .into());
    }

    bridge_server.abort();
    cas_proxy_server.abort();
    shardline_server.abort();
    Ok(())
}

async fn exercise_git_xet_clone_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let forge_root = tempfile::tempdir()?;
    let publisher_worktree = tempfile::tempdir()?;
    let clone_parent = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    let provider_config = write_generic_provider_config(storage.path())?;
    let git_project_root = forge_root.path().join("repos");
    let remote_repo_path = git_project_root.join("origin.git");
    let clone_worktree = clone_parent.path().join("clone");
    let latest_clone_worktree = clone_parent.path().join("clone-latest");
    let sparse_clone_worktree = clone_parent.path().join("clone-sparse");
    create_dir_all(&git_project_root)?;
    create_dir_all(git_home.path().join(".config"))?;

    let mut git_lfs_install = Command::new("git-lfs");
    with_test_home(&mut git_lfs_install, git_home.path()).args(["install", "--skip-repo"]);
    run_command_checked(&mut git_lfs_install, "git-lfs install")?;
    let mut git_xet_install = Command::new("git-xet");
    with_test_home(&mut git_xet_install, git_home.path()).args(["install"]);
    run_command_checked(&mut git_xet_install, "git-xet install")?;

    let shardline_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let shardline_addr = shardline_listener.local_addr()?;
    let shardline_base_url = format!("http://{shardline_addr}");
    let shardline_config = ServerConfig::new(
        shardline_addr,
        shardline_base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "generic-bridge".to_owned(),
        NonZeroU64::new(300).ok_or("token ttl")?,
    )?;
    let shardline_server =
        spawn(async move { serve_with_listener(shardline_config, shardline_listener).await });

    let client = Client::new();
    wait_for_health(&client, &shardline_base_url).await?;

    let cas_proxy_state = CasProxyState {
        upstream_base_url: shardline_base_url.clone(),
        observations: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
    };
    let cas_proxy_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let cas_proxy_addr = cas_proxy_listener.local_addr()?;
    let cas_proxy_base_url = format!("http://{cas_proxy_addr}");
    let cas_proxy_router = Router::new()
        .fallback(any(handle_cas_proxy))
        .with_state(cas_proxy_state.clone());
    let cas_proxy_server =
        spawn(async move { serve_http(cas_proxy_listener, cas_proxy_router).await });

    let bridge_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bridge_addr = bridge_listener.local_addr()?;
    let bridge_state = ForgeState {
        shardline_api_base_url: shardline_base_url.clone(),
        cas_base_url: cas_proxy_base_url.clone(),
        bridge_base_url: format!("http://{bridge_addr}"),
        provider_key: "provider-bootstrap".to_owned(),
        owner: "team".to_owned(),
        repo: "assets".to_owned(),
        requests: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
        git_project_root: git_project_root.clone(),
        storage_root: storage.path().to_path_buf(),
    };
    let forge_url = format!("http://xet:xet@{bridge_addr}");
    let bridge_router = Router::new()
        .route("/origin.git/info/lfs/objects/batch", post(handle_lfs_batch))
        .route("/lfs/download/{oid}", get(handle_lfs_download))
        .fallback(any(handle_git_http))
        .with_state(bridge_state.clone());
    let bridge_server = spawn(async move { serve_http(bridge_listener, bridge_router).await });

    run_command_checked(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(&remote_repo_path),
        "git init --bare",
    )?;
    run_command_checked(
        Command::new("git")
            .arg("--git-dir")
            .arg(&remote_repo_path)
            .args(["config", "http.receivepack", "true"]),
        "git config http.receivepack true",
    )?;

    let mut git_init = Command::new("git");
    with_test_home(&mut git_init, git_home.path())
        .current_dir(publisher_worktree.path())
        .arg("init");
    run_command_checked(&mut git_init, "git init")?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["config", "user.name", "Shardline Test"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["config", "user.email", "shardline@example.invalid"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["config", "lfs.concurrenttransfers", "1"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        [
            "remote",
            "add",
            "origin",
            &format!("{forge_url}/origin.git"),
        ],
    )?;
    let mut git_xet_track = Command::new("git-xet");
    with_test_home(&mut git_xet_track, git_home.path())
        .current_dir(publisher_worktree.path())
        .args(["track", "*.bin"]);
    run_command_checked(&mut git_xet_track, "git-xet track")?;

    let first_bytes = seeded_bytes(512 * 1024, 23);
    let second_bytes = mutated_bytes(&first_bytes, 192 * 1024, 32 * 1024, 0x41);
    let secondary_bytes = seeded_bytes(256 * 1024, 91);
    write_file(publisher_worktree.path().join("asset.bin"), &first_bytes)?;
    create_dir_all(publisher_worktree.path().join("textures"))?;
    write_file(
        publisher_worktree.path().join("textures/secondary.bin"),
        &secondary_bytes,
    )?;

    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        [
            "add",
            ".gitattributes",
            "asset.bin",
            "textures/secondary.bin",
        ],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["commit", "-m", "add xet tracked asset"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["branch", "-M", "main"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["push", "-u", "origin", "main"],
    )?;
    let stats_after_first_push = read_server_stats(&client, &shardline_base_url).await?;
    run_command_checked(
        Command::new("git")
            .arg("--git-dir")
            .arg(&remote_repo_path)
            .args(["symbolic-ref", "HEAD", "refs/heads/main"]),
        "git symbolic-ref HEAD refs/heads/main",
    )?;
    let first_commit = git_stdout_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["rev-parse", "HEAD"],
    )?;

    let mut git_clone = Command::new("git");
    with_test_home(&mut git_clone, git_home.path()).args([
        "clone",
        &format!("{forge_url}/origin.git"),
        clone_worktree.to_str().ok_or("clone path utf-8")?,
    ]);
    run_command_checked(&mut git_clone, "git clone")?;
    run_git_with_home(
        &clone_worktree,
        git_home.path(),
        ["config", "lfs.concurrenttransfers", "1"],
    )?;

    if read(clone_worktree.join("asset.bin"))? != first_bytes {
        return Err(ServerE2eInvariantError::new(
            "cloned worktree did not materialize the first asset bytes",
        )
        .into());
    }

    write_file(publisher_worktree.path().join("asset.bin"), &second_bytes)?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["add", "asset.bin"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["commit", "-m", "update xet tracked asset"],
    )?;
    run_git_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["push", "origin", "main"],
    )?;
    let stats_after_second_push = read_server_stats(&client, &shardline_base_url).await?;
    let second_commit = git_stdout_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["rev-parse", "HEAD"],
    )?;
    if stats_after_second_push.files <= stats_after_first_push.files {
        return Err(ServerE2eInvariantError::new(
            "second git-xet push did not record an additional file version",
        )
        .into());
    }
    let new_chunk_objects = stats_after_second_push
        .chunks
        .checked_sub(stats_after_first_push.chunks)
        .ok_or_else(|| {
            ServerE2eInvariantError::new("chunk object count unexpectedly shrank after second push")
        })?;
    if new_chunk_objects == 0 {
        return Err(ServerE2eInvariantError::new(
            "second git-xet push reported no new chunk objects for changed content",
        )
        .into());
    }
    if new_chunk_objects >= stats_after_first_push.chunks {
        return Err(ServerE2eInvariantError::new(
            "second git-xet push did not reuse enough existing chunks for the small update",
        )
        .into());
    }

    let mut git_clone_latest = Command::new("git");
    with_test_home(&mut git_clone_latest, git_home.path()).args([
        "clone",
        &format!("{forge_url}/origin.git"),
        latest_clone_worktree
            .to_str()
            .ok_or("latest clone path utf-8")?,
    ]);
    run_command_checked(&mut git_clone_latest, "git clone latest")?;
    if read(latest_clone_worktree.join("asset.bin"))? != second_bytes {
        return Err(ServerE2eInvariantError::new(
            "fresh clone after the second push did not materialize the latest asset bytes",
        )
        .into());
    }
    if read(latest_clone_worktree.join("textures/secondary.bin"))? != secondary_bytes {
        return Err(ServerE2eInvariantError::new(
            "fresh clone after the second push did not materialize the secondary asset bytes",
        )
        .into());
    }

    let mut git_sparse_clone = Command::new("git");
    with_test_home(&mut git_sparse_clone, git_home.path()).args([
        "clone",
        "--no-checkout",
        &format!("{forge_url}/origin.git"),
        sparse_clone_worktree
            .to_str()
            .ok_or("sparse clone path utf-8")?,
    ]);
    run_command_checked(&mut git_sparse_clone, "git sparse clone")?;
    run_git_with_home(
        &sparse_clone_worktree,
        git_home.path(),
        ["config", "lfs.concurrenttransfers", "1"],
    )?;
    run_git_with_home(
        &sparse_clone_worktree,
        git_home.path(),
        ["sparse-checkout", "init", "--no-cone"],
    )?;
    run_git_with_home(
        &sparse_clone_worktree,
        git_home.path(),
        ["sparse-checkout", "set", "asset.bin"],
    )?;
    run_git_with_home(
        &sparse_clone_worktree,
        git_home.path(),
        ["checkout", "main"],
    )?;
    if read(sparse_clone_worktree.join("asset.bin"))? != second_bytes {
        return Err(ServerE2eInvariantError::new(
            "sparse checkout did not materialize the selected xet asset bytes",
        )
        .into());
    }
    if sparse_clone_worktree
        .join("textures/secondary.bin")
        .exists()
    {
        return Err(ServerE2eInvariantError::new(
            "sparse checkout materialized a xet asset outside the sparse specification",
        )
        .into());
    }
    run_git_with_home(
        &sparse_clone_worktree,
        git_home.path(),
        [
            "sparse-checkout",
            "set",
            "asset.bin",
            "textures/secondary.bin",
        ],
    )?;
    if read(sparse_clone_worktree.join("textures/secondary.bin"))? != secondary_bytes {
        return Err(ServerE2eInvariantError::new(
            "sparse checkout expansion did not materialize the newly included xet asset bytes",
        )
        .into());
    }

    run_git_with_home(&clone_worktree, git_home.path(), ["fetch", "origin"])?;
    let fetched_origin_head = git_stdout_with_home(
        &clone_worktree,
        git_home.path(),
        ["rev-parse", "origin/main"],
    )?;
    if fetched_origin_head != second_commit {
        return Err(ServerE2eInvariantError::new(
            "git fetch did not update origin/main to the latest commit",
        )
        .into());
    }

    run_git_with_home(&clone_worktree, git_home.path(), ["pull", "--ff-only"])?;
    if read(clone_worktree.join("asset.bin"))? != second_bytes {
        return Err(ServerE2eInvariantError::new(
            "git pull did not materialize the updated asset bytes",
        )
        .into());
    }

    run_git_with_home(
        &clone_worktree,
        git_home.path(),
        ["checkout", &first_commit],
    )?;
    if read(clone_worktree.join("asset.bin"))? != first_bytes {
        return Err(ServerE2eInvariantError::new(
            "checking out the older commit did not restore the original asset bytes",
        )
        .into());
    }

    run_git_with_home(&clone_worktree, git_home.path(), ["checkout", "main"])?;
    if read(clone_worktree.join("asset.bin"))? != second_bytes {
        return Err(ServerE2eInvariantError::new(
            "checking out main did not restore the latest asset bytes",
        )
        .into());
    }

    let requests = bridge_state
        .requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("bridge request lock poisoned"))?
        .clone();
    if !requests
        .iter()
        .any(|request| request.operation == "download")
    {
        return Err(ServerE2eInvariantError::new(
            "lfs bridge did not receive any download batch requests",
        )
        .into());
    }
    if !requests
        .iter()
        .filter(|request| matches!(request.operation.as_str(), "upload" | "download"))
        .all(|request| request.transfers.iter().any(|transfer| transfer == "xet"))
    {
        return Err(ServerE2eInvariantError::new(
            "git-lfs batch requests did not consistently advertise the xet transfer",
        )
        .into());
    }

    let observations = cas_proxy_state
        .observations
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("cas observation lock poisoned"))?
        .clone();
    if !observations
        .iter()
        .any(|entry| entry.method == "GET" && entry.path_and_query.contains("/v1/chunks/default/"))
    {
        return Err(ServerE2eInvariantError::new(
            "native CAS chunk download route was not exercised during clone/fetch/pull flow",
        )
        .into());
    }

    bridge_server.abort();
    cas_proxy_server.abort();
    shardline_server.abort();
    Ok(())
}

async fn exercise_git_xet_multi_repo_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let forge_root = tempfile::tempdir()?;
    let repo_a_worktree = tempfile::tempdir()?;
    let repo_b_worktree = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    let provider_config = write_generic_provider_config_for_repositories(
        storage.path(),
        &[("team", "assets-a"), ("team", "assets-b")],
    )?;
    let git_project_root = forge_root.path().join("repos");
    let remote_repo_a_path = git_project_root.join("assets-a.git");
    let remote_repo_b_path = git_project_root.join("assets-b.git");
    create_dir_all(&git_project_root)?;
    create_dir_all(git_home.path().join(".config"))?;

    let shardline_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let shardline_addr = shardline_listener.local_addr()?;
    let shardline_base_url = format!("http://{shardline_addr}");
    let shardline_config = ServerConfig::new(
        shardline_addr,
        shardline_base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "generic-bridge".to_owned(),
        NonZeroU64::new(300).ok_or("token ttl")?,
    )?;
    let shardline_server =
        spawn(async move { serve_with_listener(shardline_config, shardline_listener).await });

    let client = Client::new();
    wait_for_health(&client, &shardline_base_url).await?;

    let cas_proxy_state = CasProxyState {
        upstream_base_url: shardline_base_url.clone(),
        observations: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
    };
    let cas_proxy_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let cas_proxy_addr = cas_proxy_listener.local_addr()?;
    let cas_proxy_base_url = format!("http://{cas_proxy_addr}");
    let cas_proxy_router = Router::new()
        .fallback(any(handle_cas_proxy))
        .with_state(cas_proxy_state.clone());
    let cas_proxy_server =
        spawn(async move { serve_http(cas_proxy_listener, cas_proxy_router).await });

    let bridge_a_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bridge_a_addr = bridge_a_listener.local_addr()?;
    let bridge_a_state = ForgeState {
        shardline_api_base_url: shardline_base_url.clone(),
        cas_base_url: cas_proxy_base_url.clone(),
        bridge_base_url: format!("http://{bridge_a_addr}"),
        provider_key: "provider-bootstrap".to_owned(),
        owner: "team".to_owned(),
        repo: "assets-a".to_owned(),
        requests: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
        git_project_root: git_project_root.clone(),
        storage_root: storage.path().to_path_buf(),
    };
    let bridge_a_url = format!("http://xet:xet@{bridge_a_addr}");
    let bridge_a_router = Router::new()
        .route(
            "/assets-a.git/info/lfs/objects/batch",
            post(handle_lfs_batch),
        )
        .route("/lfs/download/{oid}", get(handle_lfs_download))
        .fallback(any(handle_git_http))
        .with_state(bridge_a_state.clone());
    let bridge_a_server =
        spawn(async move { serve_http(bridge_a_listener, bridge_a_router).await });

    let bridge_b_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bridge_b_addr = bridge_b_listener.local_addr()?;
    let bridge_b_state = ForgeState {
        shardline_api_base_url: shardline_base_url.clone(),
        cas_base_url: cas_proxy_base_url.clone(),
        bridge_base_url: format!("http://{bridge_b_addr}"),
        provider_key: "provider-bootstrap".to_owned(),
        owner: "team".to_owned(),
        repo: "assets-b".to_owned(),
        requests: Arc::new(Mutex::new(Vec::new())),
        client: client.clone(),
        git_project_root: git_project_root.clone(),
        storage_root: storage.path().to_path_buf(),
    };
    let bridge_b_url = format!("http://xet:xet@{bridge_b_addr}");
    let bridge_b_router = Router::new()
        .route(
            "/assets-b.git/info/lfs/objects/batch",
            post(handle_lfs_batch),
        )
        .route("/lfs/download/{oid}", get(handle_lfs_download))
        .fallback(any(handle_git_http))
        .with_state(bridge_b_state.clone());
    let bridge_b_server =
        spawn(async move { serve_http(bridge_b_listener, bridge_b_router).await });

    initialize_bare_remote(&remote_repo_a_path)?;
    initialize_bare_remote(&remote_repo_b_path)?;
    initialize_git_xet_push_worktree(
        repo_a_worktree.path(),
        git_home.path(),
        &format!("{bridge_a_url}/assets-a.git"),
        &format!("{bridge_a_url}/assets-a.git/info/lfs"),
    )?;
    initialize_git_xet_push_worktree(
        repo_b_worktree.path(),
        git_home.path(),
        &format!("{bridge_b_url}/assets-b.git"),
        &format!("{bridge_b_url}/assets-b.git/info/lfs"),
    )?;

    let repo_a_bytes = seeded_bytes(512 * 1024, 29);
    let repo_b_bytes = mutated_bytes(&repo_a_bytes, 192 * 1024, 32 * 1024, 0x23);
    write_commit_and_push_asset(
        repo_a_worktree.path(),
        git_home.path(),
        &repo_a_bytes,
        "seed shared asset in repo a",
    )?;
    let stats_after_repo_a =
        read_server_stats_for_repository(&client, &shardline_base_url, "team", "assets-a").await?;
    write_commit_and_push_asset(
        repo_b_worktree.path(),
        git_home.path(),
        &repo_b_bytes,
        "seed overlapping asset in repo b",
    )?;
    let stats_after_repo_b =
        read_server_stats_for_repository(&client, &shardline_base_url, "team", "assets-b").await?;

    if stats_after_repo_b.files <= stats_after_repo_a.files {
        return Err(ServerE2eInvariantError::new(
            "second repository push did not record an additional file version",
        )
        .into());
    }
    let new_chunk_objects = stats_after_repo_b
        .chunks
        .checked_sub(stats_after_repo_a.chunks)
        .ok_or_else(|| {
            ServerE2eInvariantError::new(
                "chunk object count unexpectedly shrank after second repository push",
            )
        })?;
    if new_chunk_objects == 0 {
        return Err(ServerE2eInvariantError::new(
            "second repository push reported no new chunk objects for changed content",
        )
        .into());
    }
    if new_chunk_objects >= stats_after_repo_a.chunks {
        return Err(ServerE2eInvariantError::new(
            "second repository push did not reuse enough chunks across repositories",
        )
        .into());
    }

    assert_upload_batch_requests(&bridge_a_state.requests, "assets-a")?;
    assert_upload_batch_requests(&bridge_b_state.requests, "assets-b")?;

    let repo_a_record = read_single_version_record(storage.path(), "team", "assets-a").await?;
    let repo_b_record = read_single_version_record(storage.path(), "team", "assets-b").await?;
    let repo_a_scope = repo_a_record
        .repository_scope
        .as_ref()
        .ok_or_else(|| ServerE2eInvariantError::new("repo a record missing repository scope"))?;
    let repo_b_scope = repo_b_record
        .repository_scope
        .as_ref()
        .ok_or_else(|| ServerE2eInvariantError::new("repo b record missing repository scope"))?;
    if repo_a_scope.name() != "assets-a" || repo_b_scope.name() != "assets-b" {
        return Err(ServerE2eInvariantError::new(
            "version records were not written under the expected repository scopes",
        )
        .into());
    }

    let repo_a_read = issue_provider_token_for_repository(
        &client,
        &shardline_base_url,
        "team",
        "assets-a",
        TokenScopeRequest::Read,
    )
    .await?;
    let cross_repo_read = client
        .get(format!(
            "{shardline_base_url}/v1/reconstructions/{}?content_hash={}",
            repo_b_record.file_id, repo_b_record.content_hash
        ))
        .bearer_auth(&repo_a_read.token)
        .send()
        .await?;
    if cross_repo_read.status() != StatusCode::NOT_FOUND {
        return Err(ServerE2eInvariantError::new(format!(
            "repo a token unexpectedly read repo b reconstruction: {}",
            cross_repo_read.status()
        ))
        .into());
    }

    let repo_b_read = issue_provider_token_for_repository(
        &client,
        &shardline_base_url,
        "team",
        "assets-b",
        TokenScopeRequest::Read,
    )
    .await?;
    let repo_b_reconstruction = client
        .get(format!(
            "{shardline_base_url}/v1/reconstructions/{}?content_hash={}",
            repo_b_record.file_id, repo_b_record.content_hash
        ))
        .bearer_auth(&repo_b_read.token)
        .send()
        .await?;
    if !repo_b_reconstruction.status().is_success() {
        return Err(ServerE2eInvariantError::new(format!(
            "repo b token could not read its own reconstruction: {}",
            repo_b_reconstruction.status()
        ))
        .into());
    }

    bridge_a_server.abort();
    bridge_b_server.abort();
    cas_proxy_server.abort();
    shardline_server.abort();
    Ok(())
}

#[derive(Debug, Clone)]
struct ForgeState {
    shardline_api_base_url: String,
    cas_base_url: String,
    bridge_base_url: String,
    provider_key: String,
    owner: String,
    repo: String,
    requests: Arc<Mutex<Vec<LfsBatchRequestRecord>>>,
    client: Client,
    git_project_root: PathBuf,
    storage_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LfsBatchRequestRecord {
    operation: String,
    transfers: Vec<String>,
    object_count: usize,
}

#[derive(Debug, Clone)]
struct CasProxyState {
    upstream_base_url: String,
    observations: Arc<Mutex<Vec<CasProxyObservation>>>,
    client: Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CasProxyObservation {
    method: String,
    path_and_query: String,
    status: u16,
    body_preview: String,
}

#[derive(Debug, Deserialize)]
struct LfsBatchRequest {
    operation: String,
    transfers: Option<Vec<String>>,
    objects: Vec<LfsObjectRequest>,
}

#[derive(Debug, Deserialize)]
struct LfsObjectRequest {
    oid: String,
    size: u64,
}

#[derive(Debug, Serialize)]
struct LfsBatchResponse {
    transfer: String,
    objects: Vec<LfsObjectResponse>,
}

#[derive(Debug, Serialize)]
struct LfsObjectResponse {
    oid: String,
    size: u64,
    authenticated: bool,
    actions: Value,
}

async fn handle_lfs_batch(
    State(state): State<ForgeState>,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, (StatusCode, String)> {
    let transfers = request.transfers.clone().unwrap_or_default();
    {
        let mut recorded = state.requests.lock().map_err(|_error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "bridge request lock poisoned".to_owned(),
            )
        })?;
        recorded.push(LfsBatchRequestRecord {
            operation: request.operation.clone(),
            transfers: transfers.clone(),
            object_count: request.objects.len(),
        });
    }

    if !matches!(request.operation.as_str(), "upload" | "download") {
        return Err((
            StatusCode::BAD_REQUEST,
            "only upload and download batch requests are supported by this smoke test".to_owned(),
        ));
    }
    if !transfers.iter().any(|transfer| transfer == "xet") {
        return Err((
            StatusCode::BAD_REQUEST,
            "client did not advertise the xet transfer".to_owned(),
        ));
    }

    let issued = issue_provider_token_with_key(
        &state.client,
        &state.shardline_api_base_url,
        &state.provider_key,
        &state.owner,
        &state.repo,
        match request.operation.as_str() {
            "download" => TokenScopeRequest::Read,
            _ => TokenScopeRequest::Write,
        },
    )
    .await
    .map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to issue shardline batch token: {error}"),
        )
    })?;

    let download_operation = request.operation == "download";
    let action_name = if download_operation {
        "download"
    } else {
        "upload"
    };
    let transfer = if download_operation { "basic" } else { "xet" };
    let objects = request
        .objects
        .into_iter()
        .map(|object| LfsObjectResponse {
            oid: object.oid.clone(),
            size: object.size,
            authenticated: true,
            actions: if download_operation {
                json!({
                    action_name: {
                        "href": format!(
                            "{}/lfs/download/{}?size={}",
                            state.bridge_base_url, object.oid, object.size
                        )
                    }
                })
            } else {
                json!({
                    action_name: {
                    "href": state.cas_base_url,
                    "header": {
                        "X-Xet-Cas-Url": state.cas_base_url,
                        "X-Xet-Access-Token": issued.token,
                        "X-Xet-Token-Expiration": issued.expires_at_unix_seconds.to_string()
                    }
                }
                })
            },
        })
        .collect();

    Ok(Json(LfsBatchResponse {
        transfer: transfer.to_owned(),
        objects,
    }))
}

#[derive(Debug, Deserialize)]
struct LfsDownloadQuery {
    size: u64,
}

async fn handle_lfs_download(
    State(state): State<ForgeState>,
    AxumPath(oid): AxumPath<String>,
    Query(query): Query<LfsDownloadQuery>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let bytes = reconstruct_lfs_object(&state, &oid, query.size)
        .await
        .map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("failed to reconstruct lfs object through shardline cas: {error}"),
            )
        })?;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, bytes.len().to_string())
        .body(Body::from(bytes))
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build lfs download response: {error}"),
            )
        })
}

async fn reconstruct_lfs_object(
    state: &ForgeState,
    oid: &str,
    expected_size: u64,
) -> Result<Vec<u8>, TestError> {
    let records = read_version_records(&state.storage_root, &state.owner, &state.repo).await?;
    for record in records
        .iter()
        .filter(|record| record.total_bytes == expected_size)
    {
        let bytes = reconstruct_record_through_cas(state, record).await?;
        if sha256_hex(&bytes) == oid {
            return Ok(bytes);
        }
    }

    Err(ServerE2eInvariantError::new(
        "requested lfs object did not match any shardline file version",
    )
    .into())
}

async fn reconstruct_record_through_cas(
    state: &ForgeState,
    record: &FileRecord,
) -> Result<Vec<u8>, TestError> {
    let issued = issue_provider_token_with_key(
        &state.client,
        &state.shardline_api_base_url,
        &state.provider_key,
        &state.owner,
        &state.repo,
        TokenScopeRequest::Read,
    )
    .await?;
    let reconstruction = state
        .client
        .get(format!(
            "{}/v1/reconstructions/{}?content_hash={}",
            state.shardline_api_base_url, record.file_id, record.content_hash
        ))
        .bearer_auth(&issued.token)
        .send()
        .await?
        .error_for_status()?
        .json::<FileReconstructionResponse>()
        .await?;
    let mut output = Vec::with_capacity(usize::try_from(record.total_bytes)?);
    for term in &record.chunks {
        append_record_term_through_cas(state, &issued.token, term, &mut output).await?;
    }
    if u64::try_from(output.len())? != record.total_bytes {
        return Err(
            ServerE2eInvariantError::new("reconstructed lfs object length mismatch").into(),
        );
    }
    if reconstruction.terms.len() != record.chunks.len() {
        return Err(ServerE2eInvariantError::new("reconstruction term count mismatch").into());
    }

    Ok(output)
}

async fn append_record_term_through_cas(
    state: &ForgeState,
    token: &str,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), TestError> {
    let xorb_bytes = read_full_xorb_through_cas(state, token, &term.hash).await?;
    let expected_hash = parse_xet_hash_hex(&term.hash)?;
    let mut reader = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    let range_start = usize::try_from(term.range_start)?;
    let range_end = usize::try_from(term.range_end)?;
    let mut chunk_index = 0_usize;
    let mut appended = 0_u64;
    try_for_each_serialized_xorb_chunk(&mut reader, &validated, |decoded_chunk| {
        if chunk_index >= range_start && chunk_index < range_end {
            output.extend_from_slice(decoded_chunk.data());
            appended = appended
                .checked_add(u64::try_from(decoded_chunk.data().len()).map_err(|error| {
                    ServerE2eInvariantError::new(format!(
                        "decoded chunk length overflowed: {error}"
                    ))
                })?)
                .ok_or_else(|| {
                    ServerE2eInvariantError::new("decoded chunk length sum overflowed")
                })?;
        }
        chunk_index = chunk_index
            .checked_add(1)
            .ok_or_else(|| ServerE2eInvariantError::new("decoded xorb chunk index overflowed"))?;
        Ok::<(), IoError>(())
    })
    .map_err(ServerE2eInvariantError::new)?;
    if appended != term.length {
        return Err(ServerE2eInvariantError::new("decoded term length mismatch").into());
    }

    Ok(())
}

async fn read_full_xorb_through_cas(
    state: &ForgeState,
    token: &str,
    hash: &str,
) -> Result<Vec<u8>, TestError> {
    let head = state
        .client
        .head(format!("{}/v1/xorbs/default/{hash}", state.cas_base_url))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let length = head
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or_else(|| ServerE2eInvariantError::new("xorb head response missing content-length"))?
        .to_str()?
        .parse::<u64>()?;
    if length == 0 {
        return Err(ServerE2eInvariantError::new("xorb object length was zero").into());
    }
    let end = length
        .checked_sub(1)
        .ok_or_else(|| ServerE2eInvariantError::new("xorb length underflowed"))?;
    let response = state
        .client
        .get(format!(
            "{}/transfer/xorb/default/{hash}",
            state.cas_base_url
        ))
        .bearer_auth(token)
        .header(RANGE, format!("bytes=0-{end}"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.bytes().await?.to_vec())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

async fn handle_cas_proxy(
    State(state): State<CasProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, (StatusCode, String)> {
    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| uri.path().to_owned(), ToString::to_string);
    let upstream_url = format!("{}{}", state.upstream_base_url, path_and_query);
    let mut request = state
        .client
        .request(method.clone(), upstream_url)
        .body(body.clone());

    for (name, value) in &headers {
        if *name != HOST && *name != CONTENT_LENGTH {
            request = request.header(name, value);
        }
    }

    let response = request.send().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("cas proxy request failed: {error}"),
        )
    })?;
    let status = response.status();
    let response_headers = response.headers().clone();
    let response_bytes = response.bytes().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("cas proxy body read failed: {error}"),
        )
    })?;

    {
        let mut observations = state.observations.lock().map_err(|_error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas proxy observation lock poisoned".to_owned(),
            )
        })?;
        let preview_len = response_bytes.len().min(512);
        let preview_bytes = response_bytes.get(..preview_len).ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cas proxy response preview exceeded bounds".to_owned(),
            )
        })?;
        observations.push(CasProxyObservation {
            method: method.to_string(),
            path_and_query,
            status: status.as_u16(),
            body_preview: String::from_utf8_lossy(preview_bytes).into_owned(),
        });
    }

    let mut proxied = Response::builder().status(status);
    for (name, value) in &response_headers {
        proxied = proxied.header(name, value);
    }
    proxied.body(Body::from(response_bytes)).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cas proxy response build failed: {error}"),
        )
    })
}

async fn handle_git_http(
    State(state): State<ForgeState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, (StatusCode, String)> {
    spawn_blocking(move || {
        run_git_http_backend(
            &state.git_project_root,
            &method,
            &uri,
            &headers,
            body.as_ref(),
        )
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("git http backend task failed: {error}"),
        )
    })?
    .map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("git http backend failed: {error}"),
        )
    })
}

#[derive(Debug, Clone, Copy)]
enum TokenScopeRequest {
    Read,
    Write,
}

async fn issue_provider_token(
    client: &Client,
    shardline_base_url: &str,
    scope: TokenScopeRequest,
) -> Result<ProviderTokenIssueResponse, TestError> {
    issue_provider_token_with_key(
        client,
        shardline_base_url,
        "provider-bootstrap",
        "team",
        "assets",
        scope,
    )
    .await
}

async fn issue_provider_token_with_key(
    client: &Client,
    shardline_base_url: &str,
    provider_key: &str,
    owner: &str,
    repo: &str,
    scope: TokenScopeRequest,
) -> Result<ProviderTokenIssueResponse, TestError> {
    let response = client
        .post(format!("{shardline_base_url}/v1/providers/generic/tokens"))
        .header("x-shardline-provider-key", provider_key)
        .json(&ProviderTokenIssueRequest {
            subject: "generic-user-1".to_owned(),
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            revision: Some("main".to_owned()),
            scope: match scope {
                TokenScopeRequest::Read => TokenScope::Read,
                TokenScopeRequest::Write => TokenScope::Write,
            },
        })
        .send()
        .await?;
    let response = response.error_for_status()?;
    Ok(response.json::<ProviderTokenIssueResponse>().await?)
}

async fn issue_provider_token_for_repository(
    client: &Client,
    shardline_base_url: &str,
    owner: &str,
    repo: &str,
    scope: TokenScopeRequest,
) -> Result<ProviderTokenIssueResponse, TestError> {
    issue_provider_token_with_key(
        client,
        shardline_base_url,
        "provider-bootstrap",
        owner,
        repo,
        scope,
    )
    .await
}

async fn read_server_stats(
    client: &Client,
    shardline_base_url: &str,
) -> Result<ServerStatsResponse, TestError> {
    read_server_stats_for_repository(client, shardline_base_url, "team", "assets").await
}

async fn read_server_stats_for_repository(
    client: &Client,
    shardline_base_url: &str,
    owner: &str,
    repo: &str,
) -> Result<ServerStatsResponse, TestError> {
    let read_token = issue_provider_token_for_repository(
        client,
        shardline_base_url,
        owner,
        repo,
        TokenScopeRequest::Read,
    )
    .await?;
    Ok(client
        .get(format!("{shardline_base_url}/v1/stats"))
        .bearer_auth(&read_token.token)
        .send()
        .await?
        .error_for_status()?
        .json::<ServerStatsResponse>()
        .await?)
}

async fn wait_for_health(client: &Client, base_url: &str) -> Result<(), TestError> {
    for _attempt in 0..50 {
        if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(ServerE2eInvariantError::new("server did not become healthy").into())
}

fn run_git_http_backend(
    git_project_root: &Path,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response<Body>, TestError> {
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", git_project_root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", uri.path())
        .env("QUERY_STRING", uri.query().unwrap_or(""))
        .env(
            "CONTENT_TYPE",
            headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
        )
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("REMOTE_ADDR", "127.0.0.1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for (name, value) in headers {
        let variable = http_header_env_name(name);
        command.env(variable, value.to_str().unwrap_or(""));
    }

    let mut child = command.spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(body)?;
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(command_error("git http-backend", &output).into());
    }

    Ok(parse_cgi_response(&output.stdout)?)
}

fn http_header_env_name(name: &HeaderName) -> String {
    let mut variable = String::from("HTTP_");
    for byte in name.as_str().bytes() {
        match byte {
            b'-' => variable.push('_'),
            _ => variable.push(char::from(byte).to_ascii_uppercase()),
        }
    }
    variable
}

fn parse_cgi_response(bytes: &[u8]) -> Result<Response<Body>, ServerE2eInvariantError> {
    let (header_bytes, body_bytes) = split_cgi_response(bytes)?;
    let header_text = from_utf8(header_bytes).map_err(ServerE2eInvariantError::new)?;
    let mut status = StatusCode::OK;
    let mut response = Response::builder();

    for line in header_text.lines().filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            return Err(ServerE2eInvariantError::new(format!(
                "invalid git http-backend header line: {line}"
            )));
        };
        let trimmed_value = value.trim();
        if name.eq_ignore_ascii_case("status") {
            status = parse_status_code(trimmed_value)?;
            continue;
        }
        let header_name =
            HeaderName::from_bytes(name.trim().as_bytes()).map_err(ServerE2eInvariantError::new)?;
        let header_value =
            HeaderValue::from_str(trimmed_value).map_err(ServerE2eInvariantError::new)?;
        response = response.header(header_name, header_value);
    }

    response
        .status(status)
        .body(Body::from(body_bytes.to_vec()))
        .map_err(ServerE2eInvariantError::new)
}

fn split_cgi_response(bytes: &[u8]) -> Result<(&[u8], &[u8]), ServerE2eInvariantError> {
    if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
        let body_start = index
            .checked_add(4)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi header separator overflowed"))?;
        let header = bytes
            .get(..index)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi header bytes exceeded bounds"))?;
        let body = bytes
            .get(body_start..)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi body bytes exceeded bounds"))?;
        return Ok((header, body));
    }
    if let Some(index) = bytes.windows(2).position(|window| window == b"\n\n") {
        let body_start = index
            .checked_add(2)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi header separator overflowed"))?;
        let header = bytes
            .get(..index)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi header bytes exceeded bounds"))?;
        let body = bytes
            .get(body_start..)
            .ok_or_else(|| ServerE2eInvariantError::new("cgi body bytes exceeded bounds"))?;
        return Ok((header, body));
    }

    Err(ServerE2eInvariantError::new(
        "git http-backend response did not contain a header separator",
    ))
}

fn parse_status_code(value: &str) -> Result<StatusCode, ServerE2eInvariantError> {
    let code = value
        .split_whitespace()
        .next()
        .ok_or_else(|| ServerE2eInvariantError::new("missing status code"))?;
    let code = code.parse::<u16>().map_err(ServerE2eInvariantError::new)?;
    StatusCode::from_u16(code).map_err(ServerE2eInvariantError::new)
}

fn write_generic_provider_config(root: &Path) -> Result<PathBuf, TestError> {
    write_generic_provider_config_for_repositories(root, &[("team", "assets")])
}

fn write_generic_provider_config_for_repositories(
    root: &Path,
    repositories: &[(&str, &str)],
) -> Result<PathBuf, TestError> {
    let path = root.join("providers-generic.json");
    let repositories = repositories
        .iter()
        .map(|(owner, name)| {
            json!({
                "owner": owner,
                "name": name,
                "visibility": "private",
                "default_revision": "main",
                "clone_url": format!("https://forge.example/{owner}/{name}.git"),
                "read_subjects": ["generic-user-1"],
                "write_subjects": ["generic-user-1"]
            })
        })
        .collect::<Vec<_>>();
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "generic",
                "integration_subject": "generic-bridge",
                "webhook_secret": "secret",
                "repositories": repositories
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn initialize_bare_remote(remote_repo_path: &Path) -> Result<(), TestError> {
    let init_remote = run_command(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(remote_repo_path),
    )?;
    if !init_remote.status.success() {
        return Err(command_error("git init --bare", &init_remote).into());
    }
    run_command_checked(
        Command::new("git")
            .arg("--git-dir")
            .arg(remote_repo_path)
            .args(["config", "http.receivepack", "true"]),
        "git config http.receivepack true",
    )
}

fn initialize_git_xet_push_worktree(
    worktree: &Path,
    home: &Path,
    remote_url: &str,
    lfs_url: &str,
) -> Result<(), TestError> {
    let mut git_init = Command::new("git");
    let init_repo = run_command(
        with_test_home(&mut git_init, home)
            .current_dir(worktree)
            .arg("init"),
    )?;
    if !init_repo.status.success() {
        return Err(command_error("git init", &init_repo).into());
    }

    run_git_with_home(worktree, home, ["config", "user.name", "Shardline Test"])?;
    run_git_with_home(
        worktree,
        home,
        ["config", "user.email", "shardline@example.invalid"],
    )?;
    run_git_with_home(worktree, home, ["config", "lfs.url", lfs_url])?;
    run_git_with_home(worktree, home, ["config", "lfs.locksverify", "false"])?;
    run_git_with_home(worktree, home, ["config", "lfs.concurrenttransfers", "1"])?;
    run_git_with_home(worktree, home, ["remote", "add", "origin", remote_url])?;

    let mut git_lfs_install = Command::new("git-lfs");
    run_command_checked(
        with_test_home(&mut git_lfs_install, home)
            .current_dir(worktree)
            .args(["install", "--local"]),
        "git-lfs install --local",
    )?;
    let mut git_xet_install = Command::new("git-xet");
    run_command_checked(
        with_test_home(&mut git_xet_install, home)
            .current_dir(worktree)
            .args([
                "install",
                "--local",
                "--path",
                worktree.to_str().ok_or("worktree path utf-8")?,
            ]),
        "git-xet install --local",
    )?;
    let mut git_xet_track = Command::new("git-xet");
    run_command_checked(
        with_test_home(&mut git_xet_track, home)
            .current_dir(worktree)
            .args(["track", "*.bin"]),
        "git-xet track",
    )
}

fn write_commit_and_push_asset(
    worktree: &Path,
    home: &Path,
    asset_bytes: &[u8],
    commit_message: &str,
) -> Result<(), TestError> {
    write_file(worktree.join("asset.bin"), asset_bytes)?;
    run_git_with_home(worktree, home, ["add", ".gitattributes", "asset.bin"])?;
    run_git_with_home(worktree, home, ["commit", "-m", commit_message])?;
    run_git_with_home(worktree, home, ["branch", "-M", "main"])?;
    run_git_with_home(worktree, home, ["push", "-u", "origin", "main"])?;
    Ok(())
}

fn assert_upload_batch_requests(
    requests: &Arc<Mutex<Vec<LfsBatchRequestRecord>>>,
    repo: &str,
) -> Result<(), TestError> {
    let requests = requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("bridge request lock poisoned"))?
        .clone();
    if requests.is_empty() {
        return Err(ServerE2eInvariantError::new(format!(
            "{repo} bridge did not receive batch requests"
        ))
        .into());
    }
    if !requests.iter().any(|request| request.operation == "upload") {
        return Err(ServerE2eInvariantError::new(format!(
            "{repo} bridge did not receive an upload batch request"
        ))
        .into());
    }
    if !requests
        .iter()
        .filter(|request| request.operation == "upload")
        .all(|request| request.transfers.iter().any(|transfer| transfer == "xet"))
    {
        return Err(ServerE2eInvariantError::new(format!(
            "{repo} upload batch request did not advertise the xet transfer"
        ))
        .into());
    }

    Ok(())
}

async fn read_single_version_record(
    storage_root: &Path,
    owner: &str,
    repo: &str,
) -> Result<FileRecord, TestError> {
    let records = read_version_records(storage_root, owner, repo).await?;
    if records.len() != 1 {
        return Err(ServerE2eInvariantError::new(format!(
            "expected exactly one version record for {owner}/{repo}, found {}",
            records.len()
        ))
        .into());
    }
    records
        .into_iter()
        .next()
        .ok_or_else(|| ServerE2eInvariantError::new("version record missing").into())
}

async fn read_version_records(
    storage_root: &Path,
    owner: &str,
    repo: &str,
) -> Result<Vec<FileRecord>, TestError> {
    let record_store = LocalRecordStore::open(storage_root.to_path_buf());
    let repository_scope = RepositoryRecordScope::new(RepositoryProvider::Generic, owner, repo);
    let locators =
        RecordStore::list_repository_version_record_locators(&record_store, &repository_scope)
            .await?;

    let mut records: Vec<FileRecord> = Vec::with_capacity(locators.len());
    for locator in &locators {
        let record_bytes = RecordStore::read_record_bytes(&record_store, locator).await?;
        records.push(from_slice(&record_bytes)?);
    }
    records.sort_by(|left, right| left.file_id.cmp(&right.file_id));
    Ok(records)
}

fn seeded_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        let value = u8::try_from(index % 251).unwrap_or(0);
        bytes.push(value.wrapping_add(seed));
    }
    bytes
}

fn mutated_bytes(source: &[u8], offset: usize, len: usize, value: u8) -> Vec<u8> {
    let mut bytes = source.to_vec();
    let end = offset.saturating_add(len).min(bytes.len());
    if let Some(range) = bytes.get_mut(offset..end) {
        range.fill(value);
    }
    bytes
}

fn run_git_with_home<const N: usize>(
    repo: &Path,
    home: &Path,
    args: [&str; N],
) -> Result<(), TestError> {
    let mut command = Command::new("git");
    with_test_home(&mut command, home)
        .current_dir(repo)
        .args(args);
    run_command_checked(&mut command, "git")
}

fn git_stdout_with_home<const N: usize>(
    repo: &Path,
    home: &Path,
    args: [&str; N],
) -> Result<String, TestError> {
    let mut command = Command::new("git");
    with_test_home(&mut command, home)
        .current_dir(repo)
        .args(args);
    let output = run_command(&mut command)?;
    if !output.status.success() {
        return Err(command_error("git", &output).into());
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn with_test_home<'command>(command: &'command mut Command, home: &Path) -> &'command mut Command {
    command
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
}

fn format_lfs_requests(
    requests: &Arc<Mutex<Vec<LfsBatchRequestRecord>>>,
) -> Result<String, TestError> {
    let requests = requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("lfs request lock poisoned"))?;
    Ok(format!("{requests:#?}"))
}

fn format_cas_observations(
    observations: &Arc<Mutex<Vec<CasProxyObservation>>>,
) -> Result<String, TestError> {
    let observations = observations
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("cas observation lock poisoned"))?;
    Ok(format!("{observations:#?}"))
}

fn run_command(command: &mut Command) -> Result<Output, TestError> {
    Ok(command.output()?)
}

fn run_command_checked(command: &mut Command, description: &str) -> Result<(), TestError> {
    let output = run_command(command)?;
    if output.status.success() {
        return Ok(());
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

fn command_available(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}
