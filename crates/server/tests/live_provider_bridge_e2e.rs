mod support;

use std::{
    collections::HashMap,
    env::var,
    error::Error,
    fs::{create_dir_all, read, read_dir, write as write_file},
    io::{Cursor, Error as IoError},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http,
    http::{HeaderMap, Response, StatusCode},
    routing::{get, post},
    serve as serve_http,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hex::encode;
use reqwest::{Client, header::RANGE};
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_slice, json, to_vec};
use sha2::{Digest, Sha256};
use shardline_index::{FileChunkRecord, FileRecord};
use shardline_protocol::{
    ShardlineHash, TokenScope, try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
};
use shardline_server::{
    FileReconstructionResponse, ProviderTokenIssueRequest, ProviderTokenIssueResponse,
    ServerConfig, ServerStatsResponse, serve_with_listener,
};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, time::sleep};

type TestError = Box<dyn Error + Send + Sync>;

const LIVE_PROVIDER_BOOTSTRAP_KEY: &str = "live-provider-bootstrap-key";
const LIVE_PROVIDER_TOKEN_TTL_SECONDS: u64 = 300;
const LIVE_GITHUB_API_BASE_URL: &str = "https://api.github.com";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_provider_bridge_supports_private_github_and_gitea_repositories() {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return;
    }

    let Some(config) = LiveBridgeConfig::from_env().await else {
        return;
    };

    let result = exercise_live_provider_bridge_flow(config).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "live provider bridge e2e failed: {error:?}");
}

async fn exercise_live_provider_bridge_flow(config: LiveBridgeConfig) -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let provider_config = write_provider_config(storage.path(), &config.github, &config.gitea)?;

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
        LIVE_PROVIDER_BOOTSTRAP_KEY.as_bytes().to_vec(),
        "live-provider-bridge".to_owned(),
        NonZeroU64::new(LIVE_PROVIDER_TOKEN_TTL_SECONDS).ok_or("provider token ttl")?,
    )?;
    let shardline_server =
        spawn(async move { serve_with_listener(shardline_config, shardline_listener).await });

    let client = Client::new();
    wait_for_health(&client, &shardline_base_url).await?;

    let bridge_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bridge_addr = bridge_listener.local_addr()?;
    let bridge_base_url = format!("http://{bridge_addr}");
    let bridge_state = BridgeState {
        shardline_api_base_url: shardline_base_url.clone(),
        bridge_base_url: bridge_base_url.clone(),
        provider_key: LIVE_PROVIDER_BOOTSTRAP_KEY.to_owned(),
        client: client.clone(),
        storage_root: storage.path().to_path_buf(),
        requests: Arc::new(Mutex::new(Vec::new())),
        providers: Arc::new(HashMap::from([
            ("github".to_owned(), config.github.clone()),
            ("gitea".to_owned(), config.gitea.clone()),
        ])),
    };
    let bridge_router = Router::new()
        .route(
            "/{provider}/{owner}/{repo}/lfs/objects/batch",
            post(handle_lfs_batch),
        )
        .route(
            "/{provider}/{owner}/{repo}/lfs/download/{oid}",
            get(handle_lfs_download),
        )
        .with_state(bridge_state.clone());
    let bridge_server = spawn(async move { serve_http(bridge_listener, bridge_router).await });

    exercise_provider_repository_flow(&client, &bridge_state, &config.github).await?;
    exercise_provider_repository_flow(&client, &bridge_state, &config.gitea).await?;
    assert_recorded_downloads(&bridge_state.requests, "github", &config.github.repo)?;
    assert_recorded_downloads(&bridge_state.requests, "gitea", &config.gitea.repo)?;

    bridge_server.abort();
    shardline_server.abort();

    Ok(())
}

async fn exercise_provider_repository_flow(
    client: &Client,
    bridge_state: &BridgeState,
    config: &LiveProviderConfig,
) -> Result<(), TestError> {
    let publisher_worktree = tempfile::tempdir()?;
    let clone_worktree = tempfile::tempdir()?;
    let git_home = tempfile::tempdir()?;
    create_dir_all(git_home.path().join(".config"))?;

    let first_bytes = seeded_bytes(768 * 1024, config.seed);
    let second_bytes = mutated_bytes(&first_bytes, 224 * 1024, 48 * 1024, 0x5a);
    let first_stats =
        read_server_stats_for_repository(client, &bridge_state.shardline_api_base_url, config)
            .await?;

    initialize_worktree(
        publisher_worktree.path(),
        git_home.path(),
        config,
        &bridge_state.bridge_base_url,
    )?;

    write_commit_and_push_asset(
        publisher_worktree.path(),
        git_home.path(),
        &first_bytes,
        "add xet tracked asset",
    )?;
    let stats_after_first_push =
        read_server_stats_for_repository(client, &bridge_state.shardline_api_base_url, config)
            .await?;
    let first_chunk_delta = stats_after_first_push
        .chunks
        .checked_sub(first_stats.chunks)
        .ok_or_else(|| ServerE2eInvariantError::new("initial chunk count unexpectedly shrank"))?;
    if first_chunk_delta == 0 {
        return Err(
            ServerE2eInvariantError::new("initial push recorded no new chunk objects").into(),
        );
    }

    let first_commit = git_stdout_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["rev-parse", "HEAD"],
    )?;

    write_commit_and_push_asset(
        publisher_worktree.path(),
        git_home.path(),
        &second_bytes,
        "update xet tracked asset",
    )?;
    let second_commit = git_stdout_with_home(
        publisher_worktree.path(),
        git_home.path(),
        ["rev-parse", "HEAD"],
    )?;
    if first_commit == second_commit {
        return Err(ServerE2eInvariantError::new("second push did not create a new commit").into());
    }

    let stats_after_second_push =
        read_server_stats_for_repository(client, &bridge_state.shardline_api_base_url, config)
            .await?;
    let second_chunk_delta = stats_after_second_push
        .chunks
        .checked_sub(stats_after_first_push.chunks)
        .ok_or_else(|| ServerE2eInvariantError::new("updated chunk count unexpectedly shrank"))?;
    if second_chunk_delta == 0 {
        return Err(ServerE2eInvariantError::new(
            "second push recorded no new chunk objects for the changed region",
        )
        .into());
    }
    if second_chunk_delta >= first_chunk_delta {
        return Err(ServerE2eInvariantError::new(format!(
            "{} second push uploaded too many new chunks: first_delta={first_chunk_delta} second_delta={second_chunk_delta}",
            config.provider
        ))
        .into());
    }

    let version_records = read_version_records(
        &bridge_state.storage_root,
        &config.provider,
        &config.owner,
        &config.repo,
    )?;
    if version_records.len() < 2 {
        return Err(ServerE2eInvariantError::new(format!(
            "{} repository stored fewer than two version records",
            config.provider
        ))
        .into());
    }

    let latest_record =
        find_record_for_bytes(bridge_state, config, &version_records, &second_bytes).await?;
    if latest_record.total_bytes != u64::try_from(second_bytes.len())? {
        return Err(
            ServerE2eInvariantError::new("latest record size did not match updated asset").into(),
        );
    }

    clone_and_verify_repository(
        clone_worktree.path(),
        git_home.path(),
        config,
        &bridge_state.bridge_base_url,
        CloneVerification {
            first_commit: &first_commit,
            first_bytes: &first_bytes,
            second_bytes: &second_bytes,
        },
    )?;

    assert_recorded_uploads(&bridge_state.requests, &config.provider, &config.repo)?;

    Ok(())
}

fn initialize_worktree(
    worktree: &Path,
    git_home: &Path,
    config: &LiveProviderConfig,
    bridge_base_url: &str,
) -> Result<(), TestError> {
    let mut init_repo = Command::new("git");
    with_test_home(&mut init_repo, git_home)
        .current_dir(worktree)
        .arg("init");
    run_command_checked(&mut init_repo, "git init")?;

    run_git_with_home(
        worktree,
        git_home,
        ["config", "user.name", "Shardline Live Test"],
    )?;
    run_git_with_home(
        worktree,
        git_home,
        ["config", "user.email", "shardline-live@example.invalid"],
    )?;
    let lfs_url = config.bridge_lfs_url(bridge_base_url);
    run_git_with_home(worktree, git_home, ["config", "lfs.url", &lfs_url])?;
    run_git_with_home(worktree, git_home, ["config", "lfs.locksverify", "false"])?;
    run_git_with_home(
        worktree,
        git_home,
        ["config", "lfs.concurrenttransfers", "1"],
    )?;
    configure_bridge_auth_header(worktree, git_home, bridge_base_url, &config.lfs_auth_token)?;
    run_git_with_home(
        worktree,
        git_home,
        ["remote", "add", "origin", &config.auth_remote_url],
    )?;

    let mut git_lfs_install = Command::new("git-lfs");
    with_test_home(&mut git_lfs_install, git_home)
        .current_dir(worktree)
        .args(["install", "--local"]);
    run_command_checked(&mut git_lfs_install, "git-lfs install --local")?;

    let mut git_xet_install = Command::new("git-xet");
    with_test_home(&mut git_xet_install, git_home)
        .current_dir(worktree)
        .args([
            "install",
            "--local",
            "--path",
            worktree.to_str().ok_or("worktree path utf-8")?,
        ]);
    run_command_checked(&mut git_xet_install, "git-xet install --local")?;

    let mut git_xet_track = Command::new("git-xet");
    with_test_home(&mut git_xet_track, git_home)
        .current_dir(worktree)
        .args(["track", "*.bin"]);
    run_command_checked(&mut git_xet_track, "git-xet track")?;

    Ok(())
}

fn write_commit_and_push_asset(
    worktree: &Path,
    git_home: &Path,
    asset_bytes: &[u8],
    commit_message: &str,
) -> Result<(), TestError> {
    write_file(worktree.join("asset.bin"), asset_bytes)?;
    run_git_with_home(worktree, git_home, ["add", ".gitattributes", "asset.bin"])?;
    run_git_with_home(worktree, git_home, ["commit", "-m", commit_message])?;
    run_git_with_home(worktree, git_home, ["branch", "-M", "main"])?;
    run_git_with_home(worktree, git_home, ["push", "-u", "origin", "main"])?;
    Ok(())
}

fn clone_and_verify_repository(
    worktree: &Path,
    git_home: &Path,
    config: &LiveProviderConfig,
    bridge_base_url: &str,
    verification: CloneVerification<'_>,
) -> Result<(), TestError> {
    let mut git_clone = Command::new("git");
    with_test_home(&mut git_clone, git_home).args([
        "clone",
        "--no-checkout",
        &config.auth_remote_url,
        worktree.to_str().ok_or("clone path utf-8")?,
    ]);
    run_command_checked(&mut git_clone, "git clone --no-checkout")?;

    let lfs_url = config.bridge_lfs_url(bridge_base_url);
    run_git_with_home(worktree, git_home, ["config", "lfs.url", &lfs_url])?;
    run_git_with_home(worktree, git_home, ["config", "lfs.locksverify", "false"])?;
    run_git_with_home(
        worktree,
        git_home,
        ["config", "lfs.concurrenttransfers", "1"],
    )?;
    configure_bridge_auth_header(worktree, git_home, bridge_base_url, &config.lfs_auth_token)?;

    let mut git_lfs_install = Command::new("git-lfs");
    with_test_home(&mut git_lfs_install, git_home)
        .current_dir(worktree)
        .args(["install", "--local"]);
    run_command_checked(&mut git_lfs_install, "git-lfs install --local")?;

    let mut git_xet_install = Command::new("git-xet");
    with_test_home(&mut git_xet_install, git_home)
        .current_dir(worktree)
        .args([
            "install",
            "--local",
            "--path",
            worktree.to_str().ok_or("clone worktree path utf-8")?,
        ]);
    run_command_checked(&mut git_xet_install, "git-xet install --local")?;

    run_git_with_home(worktree, git_home, ["checkout", "main"])?;
    if read(worktree.join("asset.bin"))? != verification.second_bytes {
        return Err(ServerE2eInvariantError::new(format!(
            "{} clone did not materialize latest asset bytes",
            config.provider
        ))
        .into());
    }

    run_git_with_home(worktree, git_home, ["checkout", verification.first_commit])?;
    if read(worktree.join("asset.bin"))? != verification.first_bytes {
        return Err(ServerE2eInvariantError::new(format!(
            "{} checkout did not restore the original asset bytes",
            config.provider
        ))
        .into());
    }

    run_git_with_home(worktree, git_home, ["checkout", "main"])?;
    if read(worktree.join("asset.bin"))? != verification.second_bytes {
        return Err(ServerE2eInvariantError::new(format!(
            "{} checkout did not restore the latest asset bytes",
            config.provider
        ))
        .into());
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct BridgeState {
    shardline_api_base_url: String,
    bridge_base_url: String,
    provider_key: String,
    client: Client,
    storage_root: PathBuf,
    requests: Arc<Mutex<Vec<BridgeRequestRecord>>>,
    providers: Arc<HashMap<String, LiveProviderConfig>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BridgeRequestRecord {
    provider: String,
    owner: String,
    repo: String,
    operation: String,
}

#[derive(Debug, Clone, Copy)]
struct RepositoryAccessContext<'context> {
    provider: &'context str,
    subject: &'context str,
    owner: &'context str,
    repo: &'context str,
}

#[derive(Debug, Clone, Copy)]
struct CloneVerification<'verification> {
    first_commit: &'verification str,
    first_bytes: &'verification [u8],
    second_bytes: &'verification [u8],
}

#[derive(Debug, Clone, Deserialize)]
struct BridgeRepositoryPath {
    provider: String,
    owner: String,
    repo: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BridgeDownloadPath {
    provider: String,
    owner: String,
    repo: String,
    oid: String,
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

#[derive(Debug, Deserialize)]
struct LfsDownloadQuery {
    size: u64,
}

async fn handle_lfs_batch(
    State(state): State<BridgeState>,
    AxumPath(path): AxumPath<BridgeRepositoryPath>,
    headers: HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, (StatusCode, String)> {
    let transfers = request.transfers.clone().unwrap_or_default();
    if !matches!(request.operation.as_str(), "upload" | "download") {
        return Err((
            StatusCode::BAD_REQUEST,
            "only upload and download batch requests are supported".to_owned(),
        ));
    }
    if !transfers.iter().any(|transfer| transfer == "xet") {
        return Err((
            StatusCode::BAD_REQUEST,
            "client did not advertise the xet transfer".to_owned(),
        ));
    }

    let auth_token = extract_provider_token(&headers)?;
    let subject = resolve_provider_subject(&state, &path.provider, &auth_token).await?;

    {
        let mut recorded = state.requests.lock().map_err(|_error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "bridge request lock poisoned".to_owned(),
            )
        })?;
        recorded.push(BridgeRequestRecord {
            provider: path.provider.clone(),
            owner: path.owner.clone(),
            repo: path.repo.clone(),
            operation: request.operation.clone(),
        });
    }

    let issued = issue_provider_token_with_key(
        &state.client,
        &state.shardline_api_base_url,
        &state.provider_key,
        RepositoryAccessContext {
            provider: &path.provider,
            subject: &subject,
            owner: &path.owner,
            repo: &path.repo,
        },
        match request.operation.as_str() {
            "download" => TokenScopeRequest::Read,
            _ => TokenScopeRequest::Write,
        },
    )
    .await
    .map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to issue shardline provider token: {error}"),
        )
    })?;

    let download_operation = request.operation == "download";
    let transfer = if download_operation { "basic" } else { "xet" };
    let action_name = if download_operation {
        "download"
    } else {
        "upload"
    };
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
                            "{}/{}/{}/{}/lfs/download/{}?size={}",
                            state.bridge_base_url,
                            path.provider,
                            path.owner,
                            path.repo,
                            object.oid,
                            object.size
                        )
                    }
                })
            } else {
                json!({
                    action_name: {
                        "href": state.shardline_api_base_url,
                        "header": {
                            "X-Xet-Cas-Url": state.shardline_api_base_url,
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

async fn handle_lfs_download(
    State(state): State<BridgeState>,
    AxumPath(path): AxumPath<BridgeDownloadPath>,
    headers: HeaderMap,
    Query(query): Query<LfsDownloadQuery>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let auth_token = extract_provider_token(&headers)?;
    let subject = resolve_provider_subject(&state, &path.provider, &auth_token).await?;
    let bytes = reconstruct_lfs_object(
        &state,
        RepositoryAccessContext {
            provider: &path.provider,
            subject: &subject,
            owner: &path.owner,
            repo: &path.repo,
        },
        &path.oid,
        query.size,
    )
    .await
    .map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to reconstruct lfs object through shardline: {error}"),
        )
    })?;

    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(http::header::CONTENT_LENGTH, bytes.len().to_string())
        .body(Body::from(bytes))
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build lfs download response: {error}"),
            )
        })
}

fn extract_provider_token(headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    let header = headers
        .get(http::header::AUTHORIZATION)
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                "missing authorization header".to_owned(),
            )
        })?
        .to_str()
        .map_err(|error| {
            (
                StatusCode::UNAUTHORIZED,
                format!("authorization header was not valid utf-8: {error}"),
            )
        })?;

    if let Some(token) = header.strip_prefix("Bearer ") {
        return Ok(token.to_owned());
    }

    let Some(encoded) = header.strip_prefix("Basic ") else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "unsupported authorization scheme".to_owned(),
        ));
    };
    let decoded = BASE64_STANDARD.decode(encoded).map_err(|error| {
        (
            StatusCode::UNAUTHORIZED,
            format!("basic authorization payload was invalid: {error}"),
        )
    })?;
    let decoded = String::from_utf8(decoded).map_err(|error| {
        (
            StatusCode::UNAUTHORIZED,
            format!("basic authorization payload was not utf-8: {error}"),
        )
    })?;
    let Some((_user, token)) = decoded.split_once(':') else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "basic authorization payload did not contain a password".to_owned(),
        ));
    };
    Ok(token.to_owned())
}

async fn resolve_provider_subject(
    state: &BridgeState,
    provider: &str,
    token: &str,
) -> Result<String, (StatusCode, String)> {
    let provider_config = state.providers.get(provider).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("provider {provider} is not configured"),
        )
    })?;
    let request = state
        .client
        .get(&provider_config.api_user_url)
        .header(http::header::USER_AGENT, "shardline-live-bridge-smoke");
    let request = match provider {
        "github" => request.bearer_auth(token),
        "gitea" => request.header(http::header::AUTHORIZATION, format!("token {token}")),
        _other => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("provider {provider} is not supported"),
            ));
        }
    };
    let response = request.send().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("provider subject lookup failed: {error}"),
        )
    })?;
    let response = response.error_for_status().map_err(|error| {
        (
            StatusCode::UNAUTHORIZED,
            format!("provider rejected bridge credentials: {error}"),
        )
    })?;
    let payload = response.json::<Value>().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("provider identity response was invalid: {error}"),
        )
    })?;
    let Some(subject) = payload.get("login").and_then(Value::as_str) else {
        return Err((
            StatusCode::BAD_GATEWAY,
            "provider identity response did not include login".to_owned(),
        ));
    };
    Ok(subject.to_owned())
}

async fn reconstruct_lfs_object(
    state: &BridgeState,
    repository: RepositoryAccessContext<'_>,
    oid: &str,
    expected_size: u64,
) -> Result<Vec<u8>, TestError> {
    let records = read_version_records(
        &state.storage_root,
        repository.provider,
        repository.owner,
        repository.repo,
    )?;
    for record in records
        .iter()
        .filter(|record| record.total_bytes == expected_size)
    {
        let bytes = reconstruct_record_through_shardline(state, repository, record).await?;
        if sha256_hex(&bytes) == oid {
            return Ok(bytes);
        }
    }

    Err(
        ServerE2eInvariantError::new("requested lfs object did not match any stored file version")
            .into(),
    )
}

async fn find_record_for_bytes(
    state: &BridgeState,
    config: &LiveProviderConfig,
    records: &[FileRecord],
    expected_bytes: &[u8],
) -> Result<FileRecord, TestError> {
    for record in records {
        if record.total_bytes != u64::try_from(expected_bytes.len())? {
            continue;
        }
        let reconstructed = reconstruct_record_through_shardline(
            state,
            RepositoryAccessContext {
                provider: &config.provider,
                subject: &config.subject,
                owner: &config.owner,
                repo: &config.repo,
            },
            record,
        )
        .await?;
        if reconstructed == expected_bytes {
            return Ok(record.clone());
        }
    }

    Err(
        ServerE2eInvariantError::new("could not match stored version record to expected bytes")
            .into(),
    )
}

async fn reconstruct_record_through_shardline(
    state: &BridgeState,
    repository: RepositoryAccessContext<'_>,
    record: &FileRecord,
) -> Result<Vec<u8>, TestError> {
    let issued = issue_provider_token_with_key(
        &state.client,
        &state.shardline_api_base_url,
        &state.provider_key,
        repository,
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
        append_record_term_through_shardline(state, &issued.token, term, &mut output).await?;
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

async fn append_record_term_through_shardline(
    state: &BridgeState,
    token: &str,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), TestError> {
    let xorb_bytes = read_full_xorb_through_shardline(state, token, &term.hash).await?;
    let expected_hash = ShardlineHash::parse_api_hex(&term.hash)?;
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

async fn read_full_xorb_through_shardline(
    state: &BridgeState,
    token: &str,
    hash: &str,
) -> Result<Vec<u8>, TestError> {
    let head = state
        .client
        .head(format!(
            "{}/v1/xorbs/default/{hash}",
            state.shardline_api_base_url
        ))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let length = head
        .headers()
        .get(http::header::CONTENT_LENGTH)
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
            state.shardline_api_base_url
        ))
        .bearer_auth(token)
        .header(RANGE, format!("bytes=0-{end}"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.bytes().await?.to_vec())
}

fn assert_recorded_uploads(
    requests: &Arc<Mutex<Vec<BridgeRequestRecord>>>,
    provider: &str,
    repo: &str,
) -> Result<(), TestError> {
    let requests = requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("bridge request lock poisoned"))?
        .clone();
    if !requests.iter().any(|request| {
        request.provider == provider && request.repo == repo && request.operation == "upload"
    }) {
        return Err(ServerE2eInvariantError::new(format!(
            "{provider}/{repo} bridge did not receive an upload batch request"
        ))
        .into());
    }

    Ok(())
}

fn assert_recorded_downloads(
    requests: &Arc<Mutex<Vec<BridgeRequestRecord>>>,
    provider: &str,
    repo: &str,
) -> Result<(), TestError> {
    let requests = requests
        .lock()
        .map_err(|_error| ServerE2eInvariantError::new("bridge request lock poisoned"))?
        .clone();
    if !requests.iter().any(|request| {
        request.provider == provider && request.repo == repo && request.operation == "download"
    }) {
        return Err(ServerE2eInvariantError::new(format!(
            "{provider}/{repo} bridge did not receive a download batch request"
        ))
        .into());
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum TokenScopeRequest {
    Read,
    Write,
}

async fn issue_provider_token_with_key(
    client: &Client,
    shardline_base_url: &str,
    provider_key: &str,
    repository: RepositoryAccessContext<'_>,
    scope: TokenScopeRequest,
) -> Result<ProviderTokenIssueResponse, TestError> {
    let response = client
        .post(format!(
            "{shardline_base_url}/v1/providers/{}/tokens",
            repository.provider
        ))
        .header("x-shardline-provider-key", provider_key)
        .json(&ProviderTokenIssueRequest {
            subject: repository.subject.to_owned(),
            owner: repository.owner.to_owned(),
            repo: repository.repo.to_owned(),
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

async fn read_server_stats_for_repository(
    client: &Client,
    shardline_base_url: &str,
    config: &LiveProviderConfig,
) -> Result<ServerStatsResponse, TestError> {
    let read_token = issue_provider_token_with_key(
        client,
        shardline_base_url,
        LIVE_PROVIDER_BOOTSTRAP_KEY,
        RepositoryAccessContext {
            provider: &config.provider,
            subject: &config.subject,
            owner: &config.owner,
            repo: &config.repo,
        },
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

#[derive(Debug, Clone)]
struct LiveBridgeConfig {
    github: LiveProviderConfig,
    gitea: LiveProviderConfig,
}

impl LiveBridgeConfig {
    async fn from_env() -> Option<Self> {
        let client = Client::new();
        let github = LiveProviderConfig::github_from_env(&client).await.ok()?;
        let gitea = LiveProviderConfig::gitea_from_env(&client).await.ok()?;
        Some(Self { github, gitea })
    }
}

#[derive(Debug, Clone)]
struct LiveProviderConfig {
    provider: String,
    owner: String,
    repo: String,
    subject: String,
    seed: u8,
    api_user_url: String,
    clone_url: String,
    auth_remote_url: String,
    lfs_auth_token: String,
}

impl LiveProviderConfig {
    async fn github_from_env(client: &Client) -> Result<Self, TestError> {
        let owner = var("SHARDLINE_LIVE_GITHUB_OWNER")?;
        let repo = var("SHARDLINE_LIVE_GITHUB_REPO")?;
        let token = var("SHARDLINE_LIVE_GITHUB_TOKEN")?;
        let subject =
            resolve_live_subject(client, LIVE_GITHUB_API_BASE_URL, "github", &token).await?;
        let clone_url = format!("https://github.com/{owner}/{repo}.git");
        let encoded_token = percent_encode_component(&token);
        let auth_remote_url =
            format!("https://x-access-token:{encoded_token}@github.com/{owner}/{repo}.git");

        Ok(Self {
            provider: "github".to_owned(),
            owner,
            repo,
            subject,
            seed: 17,
            api_user_url: format!("{LIVE_GITHUB_API_BASE_URL}/user"),
            clone_url,
            auth_remote_url,
            lfs_auth_token: token,
        })
    }

    async fn gitea_from_env(client: &Client) -> Result<Self, TestError> {
        let base_url = var("SHARDLINE_LIVE_GITEA_BASE_URL")?;
        let owner = var("SHARDLINE_LIVE_GITEA_OWNER")?;
        let repo = var("SHARDLINE_LIVE_GITEA_REPO")?;
        let token = var("SHARDLINE_LIVE_GITEA_TOKEN")?;
        let subject = resolve_live_subject(client, &base_url, "gitea", &token).await?;
        let trimmed_base = base_url.trim_end_matches('/');
        let clone_url = format!("{trimmed_base}/{owner}/{repo}.git");
        let encoded_token = percent_encode_component(&token);
        let auth_remote_url = format!("{trimmed_base}/{owner}/{repo}.git");
        let auth_remote_url = auth_remote_url.replacen(
            "https://",
            &format!("https://{subject}:{encoded_token}@"),
            1,
        );

        Ok(Self {
            provider: "gitea".to_owned(),
            owner,
            repo,
            subject,
            seed: 83,
            api_user_url: format!("{trimmed_base}/api/v1/user"),
            clone_url,
            auth_remote_url,
            lfs_auth_token: token,
        })
    }

    fn bridge_lfs_url(&self, bridge_base_url: &str) -> String {
        format!(
            "http://{}/{}/{}/{}/lfs",
            bridge_base_url.trim_start_matches("http://"),
            self.provider,
            self.owner,
            self.repo
        )
    }
}

fn configure_bridge_auth_header(
    repo: &Path,
    home: &Path,
    bridge_base_url: &str,
    token: &str,
) -> Result<(), TestError> {
    let config_key = format!(
        "http.{}/.extraheader",
        bridge_base_url.trim_end_matches('/'),
    );
    let config_value = format!("Authorization: Bearer {token}");
    let mut command = Command::new("git");
    with_test_home(&mut command, home).current_dir(repo).args([
        "config",
        &config_key,
        &config_value,
    ]);
    run_command_checked(&mut command, "git config bridge auth header")
}

async fn resolve_live_subject(
    client: &Client,
    base_url: &str,
    provider: &str,
    token: &str,
) -> Result<String, TestError> {
    let user_url = match provider {
        "github" => format!("{}/user", base_url.trim_end_matches('/')),
        "gitea" => format!("{}/api/v1/user", base_url.trim_end_matches('/')),
        _other => {
            return Err(ServerE2eInvariantError::new("unsupported live provider").into());
        }
    };
    let request = client
        .get(user_url)
        .header(http::header::USER_AGENT, "shardline-live-bridge-smoke");
    let request = match provider {
        "github" => request.bearer_auth(token),
        "gitea" => request.header(http::header::AUTHORIZATION, format!("token {token}")),
        _other => request,
    };
    let response = request.send().await?.error_for_status()?;
    let payload = response.json::<Value>().await?;
    let Some(subject) = payload.get("login").and_then(Value::as_str) else {
        return Err(ServerE2eInvariantError::new(
            "provider identity payload did not contain login",
        )
        .into());
    };

    Ok(subject.to_owned())
}

fn write_provider_config(
    root: &Path,
    github: &LiveProviderConfig,
    gitea: &LiveProviderConfig,
) -> Result<PathBuf, TestError> {
    let path = root.join("providers-live.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "github",
                "integration_subject": "github-bridge",
                "webhook_secret": "replace-me",
                "repositories": [
                    {
                        "owner": github.owner,
                        "name": github.repo,
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": github.clone_url,
                        "read_subjects": [github.subject],
                        "write_subjects": [github.subject]
                    }
                ]
            },
            {
                "kind": "gitea",
                "integration_subject": "gitea-bridge",
                "webhook_secret": "replace-me",
                "repositories": [
                    {
                        "owner": gitea.owner,
                        "name": gitea.repo,
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": gitea.clone_url,
                        "read_subjects": [gitea.subject],
                        "write_subjects": [gitea.subject]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn read_version_records(
    storage_root: &Path,
    provider: &str,
    owner: &str,
    repo: &str,
) -> Result<Vec<FileRecord>, TestError> {
    let repository_root = storage_root
        .join("file_versions")
        .join(provider)
        .join(encode(owner))
        .join(encode(repo))
        .join(encode("main"));
    let mut record_paths = Vec::new();
    collect_files(&repository_root, &mut record_paths)?;
    record_paths
        .iter()
        .map(|path| {
            let record_bytes = read(path)?;
            Ok(from_slice(&record_bytes)?)
        })
        .collect()
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), TestError> {
    if !root.exists() {
        return Ok(());
    }

    for entry in read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_files(&path, files)?;
        } else if metadata.is_file() {
            files.push(path);
        }
    }

    files.sort();
    Ok(())
}

fn percent_encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
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
