mod support;

use std::{
    error::Error,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, StatusCode, Uri},
    routing::any,
    serve as serve_http,
};
use reqwest::Client;
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_server::{
    ReadyResponse, ServerConfig, ServerError, ServerFrontend, ServerRole, serve_with_listener,
};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, time::sleep};
use xet_client::cas_client::auth::AuthConfig;
use xet_data::processing::{
    FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo,
    configurations::TranslatorConfig,
};

struct FrontendRoleRuntime {
    _storage: tempfile::TempDir,
    base_url: String,
    server: tokio::task::JoinHandle<Result<(), ServerError>>,
}

impl FrontendRoleRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for FrontendRoleRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_role_serves_control_plane_routes_only() {
    let result = exercise_role(ServerRole::Api).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "api role e2e failed: {error:?}");
    let Ok((ready, recon_status, chunk_status)) = result else {
        return;
    };

    assert_eq!(ready.server_role, "api");
    assert_eq!(ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(ready.cache_backend, "memory");
    assert_eq!(recon_status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(chunk_status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_role_serves_transfer_routes_only() {
    let result = exercise_role(ServerRole::Transfer).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "transfer role e2e failed: {error:?}");
    let Ok((ready, recon_status, chunk_status)) = result else {
        return;
    };

    assert_eq!(ready.server_role, "transfer");
    assert_eq!(ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(ready.object_backend, "local");
    assert_eq!(ready.cache_backend, "disabled");
    assert_eq!(recon_status, StatusCode::NOT_FOUND);
    assert_eq!(chunk_status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_roles_support_native_xet_through_path_routed_proxy() {
    let result = exercise_split_role_native_xet_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "split role native xet e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_role_serves_oci_api_routes_but_not_oci_transfer_routes() {
    let result = exercise_oci_role_surface(ServerRole::Api).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "oci api role e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_role_serves_oci_transfer_routes_but_not_oci_api_routes() {
    let result = exercise_oci_role_surface(ServerRole::Transfer).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "oci transfer role e2e failed: {error:?}");
}

async fn exercise_role(
    role: ServerRole,
) -> Result<(ReadyResponse, StatusCode, StatusCode), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .with_server_role(role)
    .with_token_signing_key(b"signing-key".to_vec())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let client = Client::new();
    wait_for_health(&client, &base_url).await?;

    let ready = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await?
        .error_for_status()?
        .json::<ReadyResponse>()
        .await?;
    let reconstruction_status = client
        .request(
            Method::PUT,
            format!("{base_url}/v1/reconstructions/asset.bin"),
        )
        .send()
        .await?
        .status();
    let chunk_status = client
        .request(
            Method::POST,
            format!("{base_url}/v1/chunks/default-merkledb/deadbeef"),
        )
        .send()
        .await?
        .status();

    server.abort();
    Ok((ready, reconstruction_status, chunk_status))
}

async fn wait_for_health(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
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

async fn start_frontend_role_runtime(
    role: ServerRole,
    frontends: &[ServerFrontend],
) -> Result<FrontendRoleRuntime, Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .with_server_role(role)
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let client = Client::new();
    wait_for_health(&client, &base_url).await?;
    Ok(FrontendRoleRuntime {
        _storage: storage,
        base_url,
        server,
    })
}

async fn exercise_oci_role_surface(role: ServerRole) -> Result<(), Box<dyn Error>> {
    let runtime = start_frontend_role_runtime(role, &[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = bearer_token(
        "operator-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?;

    match role {
        ServerRole::Api => {
            let root = client
                .get(format!("{}/v2/", runtime.base_url()))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(root.status(), StatusCode::OK);

            let token = client
                .get(format!(
                    "{}/v2/token?service=shardline&scope=repository:team/assets:pull",
                    runtime.base_url()
                ))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(token.status(), StatusCode::OK);

            let upload = client
                .post(format!(
                    "{}/v2/team/assets/blobs/uploads",
                    runtime.base_url()
                ))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(upload.status(), StatusCode::NOT_FOUND);
        }
        ServerRole::Transfer => {
            let root = client
                .get(format!("{}/v2/", runtime.base_url()))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(root.status(), StatusCode::NOT_FOUND);

            let token = client
                .get(format!(
                    "{}/v2/token?service=shardline&scope=repository:team/assets:pull",
                    runtime.base_url()
                ))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(token.status(), StatusCode::NOT_FOUND);

            let upload = client
                .post(format!(
                    "{}/v2/team/assets/blobs/uploads",
                    runtime.base_url()
                ))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(upload.status(), StatusCode::ACCEPTED);

            let manifest = client
                .get(format!(
                    "{}/v2/team/assets/manifests/latest",
                    runtime.base_url()
                ))
                .bearer_auth(&write_token)
                .send()
                .await?;
            assert_eq!(manifest.status(), StatusCode::NOT_FOUND);
        }
        ServerRole::All => {}
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct SplitProxyState {
    access_token: String,
    api_base_url: String,
    client: Client,
    transfer_base_url: String,
}

async fn exercise_split_role_native_xet_flow() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let client_root = tempfile::tempdir()?;

    let proxy_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_base_url = format!("http://{proxy_addr}");

    let api_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let api_addr = api_listener.local_addr()?;
    let api_base_url = format!("http://{api_addr}");
    let api_config = ServerConfig::new(
        api_addr,
        proxy_base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_server_role(ServerRole::Api)
    .with_token_signing_key(b"signing-key".to_vec())?;
    let api_server = spawn(async move { serve_with_listener(api_config, api_listener).await });

    let transfer_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let transfer_addr = transfer_listener.local_addr()?;
    let transfer_base_url = format!("http://{transfer_addr}");
    let transfer_config = ServerConfig::new(
        transfer_addr,
        proxy_base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_server_role(ServerRole::Transfer)
    .with_token_signing_key(b"signing-key".to_vec())?;
    let transfer_server =
        spawn(async move { serve_with_listener(transfer_config, transfer_listener).await });

    let client = Client::new();
    wait_for_health(&client, &api_base_url).await?;
    wait_for_health(&client, &transfer_base_url).await?;
    let write_token = bearer_token(
        "operator-1",
        TokenScope::Write,
        RepositoryProvider::Generic,
        "team",
        "assets",
        Some("main"),
    )?;

    let proxy_router =
        Router::new()
            .fallback(any(handle_split_proxy))
            .with_state(SplitProxyState {
                access_token: write_token.clone(),
                api_base_url,
                client: client.clone(),
                transfer_base_url,
            });
    let proxy_server = spawn(async move { serve_http(proxy_listener, proxy_router).await });

    let translator = authenticated_translator(&proxy_base_url, client_root.path(), &write_token)?;
    let first_bytes = seeded_bytes(384 * 1024, 0x19);
    let second_bytes = mutate_bytes(&first_bytes, 128 * 1024, 160 * 1024, 0xa5)?;
    let (first_info, first_metrics) =
        upload_bytes(translator.clone(), "split-first.bin", &first_bytes).await?;
    let (second_info, second_metrics) =
        upload_bytes(translator.clone(), "split-second.bin", &second_bytes).await?;

    assert_ne!(first_info.hash(), second_info.hash());
    assert!(first_metrics.total_bytes > 0);
    assert!(second_metrics.deduped_chunks > 0);
    assert!(second_metrics.new_bytes < second_metrics.total_bytes);

    let download_session = FileDownloadSession::new(translator, None).await?;
    let first_output = client_root.path().join("split-first.out");
    let second_output = client_root.path().join("split-second.out");
    let (_first_download_id, first_downloaded) = download_session
        .download_file(&first_info, &first_output)
        .await?;
    let (_second_download_id, second_downloaded) = download_session
        .download_file(&second_info, &second_output)
        .await?;

    assert_eq!(first_downloaded, u64::try_from(first_bytes.len())?);
    assert_eq!(second_downloaded, u64::try_from(second_bytes.len())?);
    assert_eq!(fs::read(&first_output)?, first_bytes);
    assert_eq!(fs::read(&second_output)?, second_bytes);

    proxy_server.abort();
    api_server.abort();
    transfer_server.abort();
    Ok(())
}

async fn upload_bytes(
    translator: Arc<TranslatorConfig>,
    name: &str,
    bytes: &[u8],
) -> Result<(XetFileInfo, xet_data::deduplication::DeduplicationMetrics), Box<dyn Error>> {
    let upload_session = FileUploadSession::new(translator).await?;
    let (_clean_id, mut cleaner) = upload_session.start_clean(
        Some(Arc::<str>::from(name)),
        Some(u64::try_from(bytes.len())?),
        Sha256Policy::Compute,
    )?;
    cleaner.add_data(bytes).await?;
    let (file_info, cleaner_metrics) = cleaner.finish().await?;
    let session_metrics = upload_session.finalize().await?;

    let mut metrics = cleaner_metrics;
    metrics.xorb_bytes_uploaded = session_metrics.xorb_bytes_uploaded;
    metrics.shard_bytes_uploaded = session_metrics.shard_bytes_uploaded;
    metrics.total_bytes_uploaded = session_metrics.total_bytes_uploaded;

    Ok((file_info, metrics))
}

fn authenticated_translator(
    endpoint: &str,
    base_dir: &Path,
    token: &str,
) -> Result<Arc<TranslatorConfig>, Box<dyn Error>> {
    let mut translator = TranslatorConfig::test_server_config(endpoint, base_dir)?;
    translator.session.auth = AuthConfig::maybe_new(Some(token.to_owned()), Some(u64::MAX), None);
    if translator.session.auth.is_none() {
        return Err(ServerE2eInvariantError::new("failed to install xet auth config").into());
    }
    Ok(Arc::new(translator))
}

fn bearer_token(
    subject: &str,
    scope: TokenScope,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
    revision: Option<&str>,
) -> Result<String, Box<dyn Error>> {
    let signer = TokenSigner::new(b"signing-key")?;
    let repository = RepositoryScope::new(provider, owner, repo, revision)?;
    let claims = TokenClaims::new("local", subject, scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

fn seeded_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut value = seed;
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        value = value.wrapping_mul(17).wrapping_add(31);
        bytes.push(value);
    }
    bytes
}

fn mutate_bytes(
    base: &[u8],
    start: usize,
    end: usize,
    value: u8,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut updated = base.to_vec();
    let range = updated.get_mut(start..end);
    let Some(range) = range else {
        return Err(ServerE2eInvariantError::new("failed to select mutation window").into());
    };
    range.fill(value);
    Ok(updated)
}

async fn handle_split_proxy(
    State(state): State<SplitProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned());
    let upstream_base_url = select_upstream_base_url(&path_and_query, &state);
    let upstream_url = format!("{upstream_base_url}{path_and_query}");
    let has_authorization = headers.contains_key("authorization");
    let request = state
        .client
        .request(method, upstream_url)
        .headers(headers)
        .body(body);
    let request = if has_authorization {
        request
    } else {
        request.bearer_auth(&state.access_token)
    };
    let response = match request.send().await {
        Ok(built_response) => built_response,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("split proxy request failed: {error}"),
            );
        }
    };
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("split proxy body read failed: {error}"),
            );
        }
    };

    let mut response_builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        response_builder = response_builder.header(name, value);
    }

    match response_builder.body(Body::from(body_bytes)) {
        Ok(built_response) => built_response,
        Err(error) => response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("split proxy response build failed: {error}"),
        ),
    }
}

fn select_upstream_base_url<'state>(
    path_and_query: &str,
    state: &'state SplitProxyState,
) -> &'state str {
    if is_transfer_path(path_and_query) {
        &state.transfer_base_url
    } else {
        &state.api_base_url
    }
}

fn is_transfer_path(path_and_query: &str) -> bool {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _query)| path);
    path.starts_with("/v1/chunks/")
        || path.starts_with("/v1/xorbs/")
        || path.starts_with("/transfer/xorb/")
}

fn response_with_status(status: StatusCode, message: String) -> Response<Body> {
    match Response::builder().status(status).body(Body::from(message)) {
        Ok(response) => response,
        Err(_error) => Response::new(Body::from("response build failed")),
    }
}
