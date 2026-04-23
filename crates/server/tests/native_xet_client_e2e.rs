mod support;

use std::{
    env::var,
    error::Error,
    fs::{create_dir_all, read, write as write_file},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread::sleep as thread_sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, StatusCode, Uri},
    routing::any,
    serve as serve_http,
};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{from_slice, json, to_vec};
use sha2::Sha256;
use shardline_server::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    ObjectStorageAdapter, ServerConfig, XetCasTokenResponse, serve_with_listener,
};
use shardline_storage::S3ObjectStoreConfig;
use support::ServerE2eInvariantError;
use tokio::{
    net::TcpListener,
    spawn,
    task::{JoinSet, spawn_blocking},
    time::sleep,
};
use xet_client::{
    cas_client::auth::{AuthConfig, AuthError, TokenInfo, TokenRefresher},
    chunk_cache::{CacheConfig, get_cache},
};
use xet_data::processing::{
    FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo,
    configurations::TranslatorConfig,
};
use xet_runtime::core::XetRuntime;

type TestError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct RefreshRouteConfig<'config> {
    endpoint: &'config str,
    base_dir: &'config Path,
    repo: &'config str,
    token_type: &'config str,
    subject: &'config str,
    refresh_count: Arc<AtomicUsize>,
    expiration: RefreshExpiration,
}

fn threadpool() -> Option<Arc<XetRuntime>> {
    static THREADPOOL: OnceLock<Option<Arc<XetRuntime>>> = OnceLock::new();
    THREADPOOL.get_or_init(|| XetRuntime::new().ok()).clone()
}

#[test]
fn shardline_accepts_native_xet_upload_and_download_flows() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime.bridge_sync(async move { exercise_native_xet_flow(None).await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native xet e2e failed: {error:?}");
}

#[test]
fn shardline_accepts_native_xet_upload_and_download_flows_with_s3_when_configured() {
    let s3_config = optional_s3_e2e_config();
    let Some(s3_config) = s3_config else {
        return;
    };

    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result =
        runtime.bridge_sync(async move { exercise_native_xet_flow(Some(s3_config)).await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "native xet s3 e2e failed: {error:?}");
}

#[test]
fn shardline_accepts_authenticated_native_xet_repository_scoped_flows() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime.bridge_sync(async move { exercise_authenticated_native_xet_flow().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "authenticated native xet e2e failed: {error:?}"
    );
}

#[test]
fn shardline_accepts_native_xet_refresh_route_bootstrap_flows() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime.bridge_sync(async move { exercise_refresh_route_native_xet_flow().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native xet refresh-route e2e failed: {error:?}"
    );
}

#[test]
fn shardline_accepts_concurrent_authenticated_native_xet_transfer_flows() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime
        .bridge_sync(async move { exercise_concurrent_authenticated_native_xet_flow().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "concurrent authenticated native xet e2e failed: {error:?}"
    );
}

#[test]
fn shardline_accepts_long_lived_authenticated_native_xet_sessions() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime
        .bridge_sync(async move { exercise_long_lived_authenticated_native_xet_flow().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "long-lived authenticated native xet e2e failed: {error:?}"
    );
}

#[test]
fn shardline_sustains_native_xet_transfers_during_provider_lifecycle_churn() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime.bridge_sync(async move {
        exercise_native_xet_transfers_during_provider_lifecycle_churn().await
    });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native xet lifecycle churn e2e failed: {error:?}"
    );
}

#[test]
fn shardline_accepts_provider_mediated_native_xet_downloads_for_all_adapters() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result =
        runtime.bridge_sync(async move { exercise_all_provider_native_xet_downloads().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "all-provider native xet download e2e failed: {error:?}"
    );
}

#[test]
fn shardline_requires_chunk_cache_for_sparse_warm_native_xet_downloads() {
    let runtime = threadpool();
    assert!(runtime.is_some());
    let Some(runtime) = runtime else {
        return;
    };
    let result = runtime
        .bridge_sync(async move { exercise_native_xet_sparse_download_cache_regression().await });
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native xet sparse download cache regression failed: {error:?}"
    );
}

async fn exercise_native_xet_transfers_during_provider_lifecycle_churn() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_all_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::new(300).ok_or("token ttl")?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "provider-lifecycle-e2e".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let write_token = issue_provider_xet_token(
        &http_client,
        &base_url,
        ProviderXetTokenRoute {
            provider: "generic",
            owner: "team",
            repo: "assets-generic",
            token_type: "write",
            subject: "generic-user-1",
        },
    )
    .await?;
    let read_token = issue_provider_xet_token(
        &http_client,
        &base_url,
        ProviderXetTokenRoute {
            provider: "generic",
            owner: "team",
            repo: "assets-generic",
            token_type: "read",
            subject: "generic-user-1",
        },
    )
    .await?;
    let write_translator =
        authenticated_translator(&base_url, client_workdir.path(), &write_token)?;
    let read_translator = authenticated_translator(&base_url, client_workdir.path(), &read_token)?;

    let base_bytes = seeded_bytes(384 * 1024, 101);
    let (baseline_info, _baseline_metrics) = upload_bytes(
        write_translator.clone(),
        "lifecycle-baseline.bin",
        &base_bytes,
    )
    .await?;
    let upload_cases = vec![
        (
            "lifecycle-a.bin".to_owned(),
            mutate_bytes(&base_bytes, 48 * 1024, 80 * 1024, 0x41)?,
        ),
        (
            "lifecycle-b.bin".to_owned(),
            mutate_bytes(&base_bytes, 144 * 1024, 176 * 1024, 0x42)?,
        ),
        (
            "lifecycle-c.bin".to_owned(),
            mutate_bytes(&base_bytes, 240 * 1024, 272 * 1024, 0x43)?,
        ),
    ];

    let mut lifecycle_tasks = JoinSet::new();
    for webhook in generic_lifecycle_webhook_cases()? {
        let client = http_client.clone();
        let url = base_url.clone();
        lifecycle_tasks.spawn(async move {
            post_generic_webhook(&client, &url, webhook).await?;
            Ok::<_, TestError>(())
        });
    }
    let baseline_download_translator = read_translator.clone();
    let baseline_output = client_workdir.path().join("lifecycle-baseline.out");
    lifecycle_tasks.spawn(async move {
        let download_session = FileDownloadSession::new(baseline_download_translator, None).await?;
        let (_download_id, downloaded) = download_session
            .download_file(&baseline_info, &baseline_output)
            .await?;
        let actual = read(&baseline_output)?;
        Ok::<_, TestError>(
            (downloaded == u64::try_from(base_bytes.len())? && actual == base_bytes)
                .then_some(())
                .ok_or_else(|| {
                    ServerE2eInvariantError::new("baseline lifecycle download mismatch")
                })?,
        )
    });

    let mut upload_tasks = JoinSet::new();
    for (name, bytes) in upload_cases {
        let translator = write_translator.clone();
        upload_tasks.spawn(async move {
            let result = upload_bytes(translator, &name, &bytes).await?;
            Ok::<_, TestError>((name, bytes, result))
        });
    }

    let mut uploads = Vec::new();
    while let Some(result) = upload_tasks.join_next().await {
        uploads.push(result.map_err(ServerE2eInvariantError::new)??);
    }
    while let Some(result) = lifecycle_tasks.join_next().await {
        result.map_err(ServerE2eInvariantError::new)??;
    }

    assert_eq!(uploads.len(), 3);
    for (_name, _bytes, (_info, metrics)) in &uploads {
        assert!(
            metrics.deduped_chunks > 0 || metrics.new_bytes < metrics.total_bytes,
            "provider lifecycle churn upload did not reuse baseline chunks"
        );
    }

    let download_session = FileDownloadSession::new(read_translator, None).await?;
    for (name, expected_bytes, (file_info, _metrics)) in uploads {
        let output = client_workdir.path().join(format!("{name}.out"));
        let (_download_id, downloaded) =
            download_session.download_file(&file_info, &output).await?;
        assert_eq!(downloaded, u64::try_from(expected_bytes.len())?);
        assert_eq!(read(output)?, expected_bytes);
    }

    server.abort();
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProxyObservation {
    path_and_query: String,
    status: u16,
    response_bytes: u64,
    transfer_bytes: u64,
}

#[derive(Debug, Clone)]
struct ProxyState {
    client: Client,
    observations: Arc<Mutex<Vec<ProxyObservation>>>,
    proxy_base_url: String,
    upstream_base_url: String,
}

async fn exercise_native_xet_sparse_download_cache_regression() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let upload_root = tempfile::tempdir()?;
    let no_cache_root = tempfile::tempdir()?;
    let cached_root = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    );
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let upload_translator = Arc::new(TranslatorConfig::test_server_config(
        &base_url,
        upload_root.path(),
    )?);
    let proxy_listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_base_url = format!("http://{proxy_addr}");
    let proxy_state = ProxyState {
        client: http_client.clone(),
        observations: Arc::new(Mutex::new(Vec::new())),
        proxy_base_url: proxy_base_url.clone(),
        upstream_base_url: base_url.clone(),
    };
    let proxy_router = Router::new()
        .fallback(any(handle_native_xet_proxy))
        .with_state(proxy_state.clone());
    let proxy_server = spawn(async move { serve_http(proxy_listener, proxy_router).await });

    let first_bytes = seeded_bytes(384 * 1024, 17);
    let second_bytes = mutate_bytes(&first_bytes, 128 * 1024, 160 * 1024, 0x5a)?;
    let mutation_window_bytes = u64::try_from(32 * 1024)?;
    let full_file_bytes = u64::try_from(second_bytes.len())?;

    let (first_info, first_metrics) = upload_bytes(
        upload_translator.clone(),
        "cache-regression-first.bin",
        &first_bytes,
    )
    .await?;
    let (second_info, second_metrics) = upload_bytes(
        upload_translator,
        "cache-regression-second.bin",
        &second_bytes,
    )
    .await?;

    assert_ne!(first_info.hash(), second_info.hash());
    assert!(second_metrics.deduped_chunks > 0);
    assert!(second_metrics.new_bytes < second_metrics.total_bytes);
    assert!(second_metrics.new_bytes < first_metrics.new_bytes);

    let no_cache_translator = Arc::new(TranslatorConfig::test_server_config(
        &proxy_base_url,
        no_cache_root.path(),
    )?);
    let no_cache_session = FileDownloadSession::new(no_cache_translator, None).await?;
    let no_cache_first_output = no_cache_root.path().join("first.out");
    let no_cache_second_output = no_cache_root.path().join("second.out");
    let (_first_download_id, first_downloaded) = no_cache_session
        .download_file(&first_info, &no_cache_first_output)
        .await?;
    assert_eq!(first_downloaded, u64::try_from(first_bytes.len())?);
    assert_eq!(read(&no_cache_first_output)?, first_bytes);
    clear_proxy_observations(&proxy_state.observations)?;
    let (_second_download_id, second_downloaded) = no_cache_session
        .download_file(&second_info, &no_cache_second_output)
        .await?;
    assert_eq!(second_downloaded, full_file_bytes);
    assert_eq!(read(&no_cache_second_output)?, second_bytes);
    let no_cache_warm = aggregate_proxy_observations(&proxy_state.observations)?;

    let cached_translator = Arc::new(TranslatorConfig::test_server_config(
        &proxy_base_url,
        cached_root.path(),
    )?);
    let chunk_cache = get_cache(&CacheConfig {
        cache_directory: cached_root.path().join("chunk-cache"),
        cache_size: 64 * 1024 * 1024,
    })?;
    let cached_session = FileDownloadSession::new(cached_translator, Some(chunk_cache)).await?;
    let cached_first_output = cached_root.path().join("first.out");
    let cached_second_output = cached_root.path().join("second.out");
    clear_proxy_observations(&proxy_state.observations)?;
    let (_cached_first_download_id, cached_first_downloaded) = cached_session
        .download_file(&first_info, &cached_first_output)
        .await?;
    assert_eq!(cached_first_downloaded, u64::try_from(first_bytes.len())?);
    assert_eq!(read(&cached_first_output)?, first_bytes);
    clear_proxy_observations(&proxy_state.observations)?;
    let (_cached_second_download_id, cached_second_downloaded) = cached_session
        .download_file(&second_info, &cached_second_output)
        .await?;
    assert_eq!(cached_second_downloaded, full_file_bytes);
    assert_eq!(read(&cached_second_output)?, second_bytes);
    let cached_warm = aggregate_proxy_observations(&proxy_state.observations)?;

    assert!(no_cache_warm.transfer_bytes > 0);
    assert!(cached_warm.transfer_bytes > 0);
    assert!(
        no_cache_warm.transfer_bytes >= full_file_bytes.saturating_mul(3) / 4,
        "warm native-xet transfer without chunk cache was unexpectedly sparse: transfer={}, file={}",
        no_cache_warm.transfer_bytes,
        full_file_bytes
    );
    assert!(
        cached_warm.transfer_bytes < no_cache_warm.transfer_bytes,
        "chunk cache did not reduce warm native-xet transfer: cached={}, no_cache={}",
        cached_warm.transfer_bytes,
        no_cache_warm.transfer_bytes
    );
    assert!(
        cached_warm.transfer_bytes < full_file_bytes,
        "warm native-xet transfer with chunk cache downloaded the full file: transfer={}, file={}",
        cached_warm.transfer_bytes,
        full_file_bytes
    );
    assert!(
        cached_warm.transfer_bytes <= mutation_window_bytes.saturating_mul(8),
        "warm native-xet transfer with chunk cache exceeded sparse-update budget: cached={}, mutation_window={}",
        cached_warm.transfer_bytes,
        mutation_window_bytes
    );

    proxy_server.abort();
    server.abort();
    Ok(())
}

async fn exercise_native_xet_flow(s3_config: Option<S3ObjectStoreConfig>) -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let mut config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    );
    if let Some(s3_config) = s3_config {
        config = config.with_object_storage(ObjectStorageAdapter::S3, Some(s3_config));
    }
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let translator = Arc::new(TranslatorConfig::test_server_config(
        &base_url,
        client_workdir.path(),
    )?);

    let first_bytes = seeded_bytes(384 * 1024, 7);
    let mut second_bytes = first_bytes.clone();
    let middle = second_bytes.get_mut(128 * 1024..160 * 1024);
    assert!(middle.is_some());
    let Some(middle) = middle else {
        return Err(ServerE2eInvariantError::new("failed to select modified byte range").into());
    };
    middle.fill(0x5a);

    let (first_info, first_metrics) =
        upload_bytes(translator.clone(), "first.bin", &first_bytes).await?;
    let (second_info, second_metrics) =
        upload_bytes(translator.clone(), "second.bin", &second_bytes).await?;

    assert_ne!(first_info.hash(), second_info.hash());
    assert!(second_metrics.deduped_chunks > 0);
    assert!(second_metrics.new_bytes < second_metrics.total_bytes);
    assert!(second_metrics.new_bytes < first_metrics.new_bytes);

    let reconstruction = http_client
        .get(format!(
            "{base_url}/v2/reconstructions/{}",
            second_info.hash()
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<FileReconstructionV2Response>()
        .await?;
    assert!(!reconstruction.terms.is_empty());
    assert!(!reconstruction.xorbs.is_empty());

    let batch = http_client
        .get(format!(
            "{base_url}/reconstructions?file_id={}&file_id={}",
            first_info.hash(),
            second_info.hash()
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<BatchReconstructionResponse>()
        .await?;
    assert_eq!(batch.files.len(), 2);

    let first_xorb = reconstruction.xorbs.keys().next().ok_or("missing xorb")?;
    let head = http_client
        .head(format!("{base_url}/v1/xorbs/default/{first_xorb}"))
        .send()
        .await?;
    assert!(head.status().is_success());

    let download_session = FileDownloadSession::new(translator, None).await?;
    let first_output = client_workdir.path().join("first.out");
    let second_output = client_workdir.path().join("second.out");
    let (_first_download_id, first_downloaded) = download_session
        .download_file(&first_info, &first_output)
        .await?;
    let (_second_download_id, second_downloaded) = download_session
        .download_file(&second_info, &second_output)
        .await?;

    assert_eq!(first_downloaded, u64::try_from(first_bytes.len())?);
    assert_eq!(second_downloaded, u64::try_from(second_bytes.len())?);
    assert_eq!(read(&first_output)?, first_bytes);
    assert_eq!(read(&second_output)?, second_bytes);

    server.abort();
    Ok(())
}

async fn upload_bytes(
    translator: Arc<TranslatorConfig>,
    name: &str,
    bytes: &[u8],
) -> Result<(XetFileInfo, xet_data::deduplication::DeduplicationMetrics), TestError> {
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

async fn handle_native_xet_proxy(
    State(state): State<ProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned());
    let upstream_url = format!("{}{}", state.upstream_base_url, path_and_query);
    let request = state
        .client
        .request(method, &upstream_url)
        .headers(headers)
        .body(body);
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("native xet proxy request failed: {error}"),
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
                format!("native xet proxy body read failed: {error}"),
            );
        }
    };
    let rewritten_body = match rewrite_reconstruction_body(
        &path_and_query,
        body_bytes,
        &state.proxy_base_url,
        &state.upstream_base_url,
    ) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("native xet proxy rewrite failed: {error}"),
            );
        }
    };
    let transfer_bytes = if path_and_query.starts_with("/transfer/xorb/")
        || path_and_query.starts_with("/v1/chunks/default/")
    {
        u64::try_from(rewritten_body.len()).unwrap_or(u64::MAX)
    } else {
        0
    };
    if let Ok(mut observations) = state.observations.lock() {
        observations.push(ProxyObservation {
            path_and_query,
            status: status.as_u16(),
            response_bytes: u64::try_from(rewritten_body.len()).unwrap_or(u64::MAX),
            transfer_bytes,
        });
    }

    let mut response_builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        response_builder = response_builder.header(name, value);
    }
    match response_builder.body(Body::from(rewritten_body)) {
        Ok(built_response) => built_response,
        Err(error) => response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("native xet proxy response build failed: {error}"),
        ),
    }
}

fn rewrite_reconstruction_body(
    path_and_query: &str,
    body: Bytes,
    proxy_base_url: &str,
    upstream_base_url: &str,
) -> Result<Bytes, TestError> {
    if path_and_query.starts_with("/v2/reconstructions/") {
        let mut reconstruction = from_slice::<FileReconstructionV2Response>(&body)?;
        for fetches in reconstruction.xorbs.values_mut() {
            for fetch in fetches {
                if fetch.url.starts_with(upstream_base_url) {
                    fetch.url = fetch.url.replacen(upstream_base_url, proxy_base_url, 1);
                }
            }
        }
        return Ok(Bytes::from(to_vec(&reconstruction)?));
    }
    if path_and_query.starts_with("/v1/reconstructions/") {
        let mut reconstruction = from_slice::<FileReconstructionResponse>(&body)?;
        for fetches in reconstruction.fetch_info.values_mut() {
            for fetch in fetches {
                if fetch.url.starts_with(upstream_base_url) {
                    fetch.url = fetch.url.replacen(upstream_base_url, proxy_base_url, 1);
                }
            }
        }
        return Ok(Bytes::from(to_vec(&reconstruction)?));
    }
    Ok(body)
}

fn aggregate_proxy_observations(
    observations: &Arc<Mutex<Vec<ProxyObservation>>>,
) -> Result<ProxyObservation, TestError> {
    let observations = observations.lock().map_err(|_error| {
        ServerE2eInvariantError::new("native xet proxy observation lock poisoned")
    })?;
    let mut combined = ProxyObservation {
        path_and_query: "aggregate".to_owned(),
        status: StatusCode::OK.as_u16(),
        response_bytes: 0,
        transfer_bytes: 0,
    };
    for observation in observations.iter() {
        if observation.status >= StatusCode::BAD_REQUEST.as_u16() {
            return Err(ServerE2eInvariantError::new(format!(
                "native xet proxy observed failed upstream request: {} {}",
                observation.status, observation.path_and_query
            ))
            .into());
        }
        combined.response_bytes = combined
            .response_bytes
            .checked_add(observation.response_bytes)
            .ok_or_else(|| {
                ServerE2eInvariantError::new("native xet proxy response byte total overflowed")
            })?;
        combined.transfer_bytes = combined
            .transfer_bytes
            .checked_add(observation.transfer_bytes)
            .ok_or_else(|| {
                ServerE2eInvariantError::new("native xet proxy transfer byte total overflowed")
            })?;
    }
    Ok(combined)
}

fn clear_proxy_observations(
    observations: &Arc<Mutex<Vec<ProxyObservation>>>,
) -> Result<(), TestError> {
    let mut observations = observations.lock().map_err(|_error| {
        ServerE2eInvariantError::new("native xet proxy observation lock poisoned")
    })?;
    observations.clear();
    Ok(())
}

fn response_with_status(status: StatusCode, message: String) -> Response<Body> {
    match Response::builder().status(status).body(Body::from(message)) {
        Ok(response) => response,
        Err(_error) => Response::new(Body::from("response build failed")),
    }
}

async fn exercise_all_provider_native_xet_downloads() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_root = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_all_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::new(300).ok_or("token ttl")?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "provider-download-e2e".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    for (index, provider) in all_provider_cases().iter().enumerate() {
        let write_token = issue_provider_xet_token(
            &http_client,
            &base_url,
            ProviderXetTokenRoute {
                provider: provider.kind,
                owner: provider.owner,
                repo: provider.repo,
                token_type: "write",
                subject: provider.subject,
            },
        )
        .await?;
        let read_token = issue_provider_xet_token(
            &http_client,
            &base_url,
            ProviderXetTokenRoute {
                provider: provider.kind,
                owner: provider.owner,
                repo: provider.repo,
                token_type: "read",
                subject: provider.subject,
            },
        )
        .await?;
        let provider_workdir = client_root.path().join(provider.kind);
        create_dir_all(&provider_workdir)?;
        let write_translator =
            authenticated_translator(&base_url, &provider_workdir, &write_token)?;
        let read_translator = authenticated_translator(&base_url, &provider_workdir, &read_token)?;
        let length = 192_usize
            .checked_mul(1024)
            .and_then(|base| {
                index
                    .checked_mul(4096)
                    .and_then(|delta| base.checked_add(delta))
            })
            .ok_or_else(|| ServerE2eInvariantError::new("provider test length overflowed"))?;
        let bytes = seeded_bytes(length, provider.seed);
        let (file_info, metrics) = upload_bytes(
            write_translator,
            &format!("{}-asset.bin", provider.kind),
            &bytes,
        )
        .await?;
        assert!(metrics.total_bytes > 0);

        let download_session = FileDownloadSession::new(read_translator, None).await?;
        let output = provider_workdir.join(format!("{}-asset.out", provider.kind));
        let (_download_id, downloaded) =
            download_session.download_file(&file_info, &output).await?;
        assert_eq!(downloaded, u64::try_from(bytes.len())?);
        assert_eq!(read(output)?, bytes);
    }

    server.abort();
    Ok(())
}

async fn exercise_authenticated_native_xet_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let repo_a_client = tempfile::tempdir()?;
    let repo_b_client = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::new(300).ok_or("token ttl")?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "github-app".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let repo_a_write = issue_xet_token(
        &http_client,
        &base_url,
        "assets-a",
        "write",
        "github-user-1",
    )
    .await?;
    let repo_a_read =
        issue_xet_token(&http_client, &base_url, "assets-a", "read", "github-user-1").await?;
    let repo_b_write = issue_xet_token(
        &http_client,
        &base_url,
        "assets-b",
        "write",
        "github-user-2",
    )
    .await?;
    let repo_b_read =
        issue_xet_token(&http_client, &base_url, "assets-b", "read", "github-user-2").await?;

    let repo_a_write_translator =
        authenticated_translator(&base_url, repo_a_client.path(), &repo_a_write)?;
    let repo_a_read_translator =
        authenticated_translator(&base_url, repo_a_client.path(), &repo_a_read)?;
    let repo_b_write_translator =
        authenticated_translator(&base_url, repo_b_client.path(), &repo_b_write)?;
    let repo_b_read_translator =
        authenticated_translator(&base_url, repo_b_client.path(), &repo_b_read)?;

    let first_repo_a_bytes = seeded_bytes(384 * 1024, 19);
    let mut second_repo_a_bytes = first_repo_a_bytes.clone();
    let updated_window = second_repo_a_bytes.get_mut(144 * 1024..176 * 1024);
    assert!(updated_window.is_some());
    let Some(updated_window) = updated_window else {
        server.abort();
        return Err(ServerE2eInvariantError::new("failed to select updated byte range").into());
    };
    updated_window.fill(0xa5);
    let repo_b_bytes = seeded_bytes(192 * 1024, 71);

    let (first_repo_a_info, first_repo_a_metrics) = upload_bytes(
        repo_a_write_translator.clone(),
        "repo-a-first.bin",
        &first_repo_a_bytes,
    )
    .await?;
    let (second_repo_a_info, second_repo_a_metrics) = upload_bytes(
        repo_a_write_translator,
        "repo-a-second.bin",
        &second_repo_a_bytes,
    )
    .await?;
    let (repo_b_info, repo_b_metrics) =
        upload_bytes(repo_b_write_translator, "repo-b.bin", &repo_b_bytes).await?;

    assert_ne!(first_repo_a_info.hash(), second_repo_a_info.hash());
    assert!(second_repo_a_metrics.deduped_chunks > 0);
    assert!(second_repo_a_metrics.new_bytes < second_repo_a_metrics.total_bytes);
    assert!(second_repo_a_metrics.new_bytes < first_repo_a_metrics.new_bytes);
    assert!(repo_b_metrics.total_bytes > 0);

    let read_scoped_upload = upload_bytes(
        repo_a_read_translator.clone(),
        "should-not-upload.bin",
        &first_repo_a_bytes,
    )
    .await;
    assert!(read_scoped_upload.is_err());

    let repo_a_download = FileDownloadSession::new(repo_a_read_translator.clone(), None).await?;
    let repo_b_download = FileDownloadSession::new(repo_b_read_translator, None).await?;
    let repo_a_output = repo_a_client.path().join("repo-a.out");
    let repo_b_output = repo_b_client.path().join("repo-b.out");
    let (_repo_a_download_id, repo_a_downloaded) = repo_a_download
        .download_file(&second_repo_a_info, &repo_a_output)
        .await?;
    let (_repo_b_download_id, repo_b_downloaded) = repo_b_download
        .download_file(&repo_b_info, &repo_b_output)
        .await?;

    assert_eq!(repo_a_downloaded, u64::try_from(second_repo_a_bytes.len())?);
    assert_eq!(repo_b_downloaded, u64::try_from(repo_b_bytes.len())?);
    assert_eq!(read(&repo_a_output)?, second_repo_a_bytes);
    assert_eq!(read(&repo_b_output)?, repo_b_bytes);

    let (_range_download_id, mut range_stream) = repo_a_download
        .download_stream_range(&second_repo_a_info, 150_000_u64..182_000_u64)
        .await?;
    let mut ranged = Vec::new();
    while let Some(chunk) = range_stream.next().await? {
        ranged.extend_from_slice(&chunk);
    }
    let expected_range = second_repo_a_bytes.get(150_000..182_000);
    assert!(expected_range.is_some());
    let Some(expected_range) = expected_range else {
        server.abort();
        return Err(ServerE2eInvariantError::new("failed to build expected ranged bytes").into());
    };
    assert_eq!(ranged, expected_range);

    let cross_scope_download = repo_a_download
        .download_file(
            &repo_b_info,
            &repo_a_client.path().join("repo-b-forbidden.out"),
        )
        .await;
    assert!(cross_scope_download.is_err());

    server.abort();
    Ok(())
}

async fn exercise_refresh_route_native_xet_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::new(300).ok_or("token ttl")?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "github-app".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let write_refresh_count = Arc::new(AtomicUsize::new(0));
    let read_refresh_count = Arc::new(AtomicUsize::new(0));
    let upload_translator = refreshing_translator(
        &base_url,
        client_workdir.path(),
        "assets-a",
        "write",
        "github-user-1",
        write_refresh_count.clone(),
    )?;
    let download_translator = refreshing_translator(
        &base_url,
        client_workdir.path(),
        "assets-a",
        "read",
        "github-user-1",
        read_refresh_count.clone(),
    )?;

    let first_bytes = seeded_bytes(320 * 1024, 13);
    let mut second_bytes = first_bytes.clone();
    let mutation_window = second_bytes.get_mut(96 * 1024..128 * 1024);
    assert!(mutation_window.is_some());
    let Some(mutation_window) = mutation_window else {
        server.abort();
        return Err(ServerE2eInvariantError::new("failed to select mutation window").into());
    };
    mutation_window.fill(0x33);

    let (first_info, first_metrics) =
        upload_bytes(upload_translator.clone(), "refresh-first.bin", &first_bytes).await?;
    let (second_info, second_metrics) =
        upload_bytes(upload_translator, "refresh-second.bin", &second_bytes).await?;

    assert_ne!(first_info.hash(), second_info.hash());
    assert!(second_metrics.deduped_chunks > 0);
    assert!(second_metrics.new_bytes < second_metrics.total_bytes);
    assert!(second_metrics.new_bytes < first_metrics.new_bytes);

    let download_session = FileDownloadSession::new(download_translator, None).await?;
    let output = client_workdir.path().join("refresh-route.out");
    let (_download_id, downloaded) = download_session
        .download_file(&second_info, &output)
        .await?;
    assert_eq!(downloaded, u64::try_from(second_bytes.len())?);
    assert_eq!(read(&output)?, second_bytes);

    assert!(
        write_refresh_count.load(Ordering::Relaxed) > 0,
        "write token refresher was never called"
    );
    assert!(
        read_refresh_count.load(Ordering::Relaxed) > 0,
        "read token refresher was never called"
    );

    server.abort();
    Ok(())
}

async fn exercise_concurrent_authenticated_native_xet_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::new(300).ok_or("token ttl")?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "github-app".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let write_refresh_count = Arc::new(AtomicUsize::new(0));
    let read_refresh_count = Arc::new(AtomicUsize::new(0));
    let upload_translator = refreshing_translator(
        &base_url,
        client_workdir.path(),
        "assets-a",
        "write",
        "github-user-1",
        write_refresh_count.clone(),
    )?;
    let download_translator = refreshing_translator(
        &base_url,
        client_workdir.path(),
        "assets-a",
        "read",
        "github-user-1",
        read_refresh_count.clone(),
    )?;

    let base_bytes = seeded_bytes(448 * 1024, 29);
    let upload_cases = vec![
        (
            "concurrent-a.bin".to_owned(),
            mutate_bytes(&base_bytes, 64 * 1024, 96 * 1024, 0x11)?,
        ),
        (
            "concurrent-b.bin".to_owned(),
            mutate_bytes(&base_bytes, 128 * 1024, 160 * 1024, 0x22)?,
        ),
        (
            "concurrent-c.bin".to_owned(),
            mutate_bytes(&base_bytes, 192 * 1024, 224 * 1024, 0x33)?,
        ),
        (
            "concurrent-d.bin".to_owned(),
            mutate_bytes(&base_bytes, 256 * 1024, 288 * 1024, 0x44)?,
        ),
    ];

    let mut upload_tasks = JoinSet::new();
    for (name, bytes) in upload_cases {
        let translator = upload_translator.clone();
        upload_tasks.spawn(async move {
            let result = upload_bytes(translator, &name, &bytes).await?;
            Ok::<_, TestError>((name, bytes, result))
        });
    }

    let mut uploads = Vec::new();
    while let Some(result) = upload_tasks.join_next().await {
        uploads.push(result.map_err(ServerE2eInvariantError::new)??);
    }

    assert_eq!(uploads.len(), 4);
    for (index, (_name, _bytes, (info, _metrics))) in uploads.iter().enumerate() {
        for (other_index, (_other_name, _other_bytes, (other_info, _other_metrics))) in
            uploads.iter().enumerate()
        {
            if index != other_index {
                assert_ne!(info.hash(), other_info.hash());
            }
        }
    }

    let repeated_bytes = uploads
        .first()
        .map(|(_name, bytes, (_info, _metrics))| bytes.clone())
        .ok_or_else(|| ServerE2eInvariantError::new("missing concurrent upload sample"))?;
    let (_repeat_info, repeat_metrics) = upload_bytes(
        upload_translator.clone(),
        "concurrent-repeat.bin",
        &repeated_bytes,
    )
    .await?;
    assert!(
        repeat_metrics.deduped_chunks > 0 || repeat_metrics.new_bytes < repeat_metrics.total_bytes,
        "repeat upload after concurrent batch did not report chunk reuse"
    );

    let mut download_tasks = JoinSet::new();
    for (name, expected_bytes, (file_info, _metrics)) in uploads {
        let translator = download_translator.clone();
        let output_path = client_workdir.path().join(format!("{name}.out"));
        download_tasks.spawn(async move {
            let session = FileDownloadSession::new(translator, None).await?;
            let (_download_id, downloaded) =
                session.download_file(&file_info, &output_path).await?;
            let actual = read(&output_path)?;
            Ok::<_, TestError>((expected_bytes, actual, downloaded))
        });
    }

    let mut completed_downloads = 0_usize;
    while let Some(result) = download_tasks.join_next().await {
        let (expected, actual, downloaded) = result.map_err(ServerE2eInvariantError::new)??;
        assert_eq!(downloaded, u64::try_from(expected.len())?);
        assert_eq!(actual, expected);
        completed_downloads = completed_downloads
            .checked_add(1)
            .ok_or_else(|| ServerE2eInvariantError::new("completed download counter overflowed"))?;
    }
    assert_eq!(completed_downloads, 4);

    assert!(
        write_refresh_count.load(Ordering::Relaxed) > 1,
        "concurrent write token refresher activity was not observed"
    );
    assert!(
        read_refresh_count.load(Ordering::Relaxed) > 1,
        "concurrent read token refresher activity was not observed"
    );

    server.abort();
    Ok(())
}

async fn exercise_long_lived_authenticated_native_xet_flow() -> Result<(), TestError> {
    let storage = tempfile::tempdir()?;
    let client_workdir = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let provider_config = write_provider_config(storage.path())?;
    let token_ttl = NonZeroU64::MIN;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_provider_runtime(
        provider_config,
        b"provider-bootstrap".to_vec(),
        "github-app".to_owned(),
        token_ttl,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let http_client = Client::new();
    wait_for_health(&http_client, &base_url).await?;

    let write_refresh_count = Arc::new(AtomicUsize::new(0));
    let read_refresh_count = Arc::new(AtomicUsize::new(0));
    let upload_translator = refreshing_translator_with_expiration(RefreshRouteConfig {
        endpoint: &base_url,
        base_dir: client_workdir.path(),
        repo: "assets-a",
        token_type: "write",
        subject: "github-user-1",
        refresh_count: write_refresh_count.clone(),
        expiration: RefreshExpiration::Issued,
    })?;
    let download_translator = refreshing_translator_with_expiration(RefreshRouteConfig {
        endpoint: &base_url,
        base_dir: client_workdir.path(),
        repo: "assets-a",
        token_type: "read",
        subject: "github-user-1",
        refresh_count: read_refresh_count.clone(),
        expiration: RefreshExpiration::Issued,
    })?;

    let base_bytes = seeded_bytes(320 * 1024, 41);
    let download_session = FileDownloadSession::new(download_translator.clone(), None).await?;

    for round in 0..4_usize {
        let kib = 1_024_usize;
        let base_start = 32_usize
            .checked_mul(kib)
            .ok_or_else(|| ServerE2eInvariantError::new("base start calculation overflowed"))?;
        let round_stride = 48_usize
            .checked_mul(kib)
            .ok_or_else(|| ServerE2eInvariantError::new("round stride calculation overflowed"))?;
        let round_offset = round
            .checked_mul(round_stride)
            .ok_or_else(|| ServerE2eInvariantError::new("round offset calculation overflowed"))?;
        let start = base_start
            .checked_add(round_offset)
            .ok_or_else(|| ServerE2eInvariantError::new("mutation start calculation overflowed"))?;
        let mutation_width = 32_usize
            .checked_mul(kib)
            .ok_or_else(|| ServerE2eInvariantError::new("mutation width calculation overflowed"))?;
        let end = start
            .checked_add(mutation_width)
            .ok_or_else(|| ServerE2eInvariantError::new("mutation end calculation overflowed"))?;
        let raw_value = 0x20_usize
            .checked_add(round)
            .ok_or_else(|| ServerE2eInvariantError::new("mutation value calculation overflowed"))?;
        let value = u8::try_from(raw_value)
            .map_err(|_error| ServerE2eInvariantError::new("mutation value exceeded u8 bounds"))?;
        let updated = mutate_bytes(&base_bytes, start, end, value)?;
        let file_name = format!("long-lived-{round}.bin");
        let (file_info, metrics) =
            upload_bytes(upload_translator.clone(), &file_name, &updated).await?;

        if round > 0 {
            assert!(
                metrics.deduped_chunks > 0 || metrics.new_bytes < metrics.total_bytes,
                "sustained upload round did not report reuse"
            );
        }

        let output = client_workdir.path().join(format!("{file_name}.out"));
        let (_download_id, downloaded) =
            download_session.download_file(&file_info, &output).await?;
        assert_eq!(downloaded, u64::try_from(updated.len())?);
        assert_eq!(read(&output)?, updated);

        let range_start = 16_384_u64;
        let range_end = 32_768_u64;
        let (_range_download_id, mut range_stream) = download_session
            .download_stream_range(&file_info, range_start..range_end)
            .await?;
        let mut ranged = Vec::new();
        while let Some(chunk) = range_stream.next().await? {
            ranged.extend_from_slice(&chunk);
        }
        let expected_ranged = updated
            .get(usize::try_from(range_start)?..usize::try_from(range_end)?)
            .ok_or_else(|| {
                ServerE2eInvariantError::new("failed to build expected sustained ranged bytes")
            })?;
        assert_eq!(ranged, expected_ranged);

        spawn_blocking(|| thread_sleep(Duration::from_millis(1_250)))
            .await
            .map_err(ServerE2eInvariantError::new)?;
    }

    assert!(
        write_refresh_count.load(Ordering::Relaxed) >= 4,
        "long-lived write token refreshes were not observed across expirations"
    );
    assert!(
        read_refresh_count.load(Ordering::Relaxed) >= 4,
        "long-lived read token refreshes were not observed across expirations"
    );

    server.abort();
    Ok(())
}

fn authenticated_translator(
    endpoint: &str,
    base_dir: &Path,
    token: &XetCasTokenResponse,
) -> Result<Arc<TranslatorConfig>, TestError> {
    let mut translator = TranslatorConfig::test_server_config(endpoint, base_dir)?;
    translator.session.auth =
        AuthConfig::maybe_new(Some(token.access_token.clone()), Some(token.exp), None);
    if translator.session.auth.is_none() {
        return Err(ServerE2eInvariantError::new("failed to install xet auth config").into());
    }
    Ok(Arc::new(translator))
}

fn refreshing_translator(
    endpoint: &str,
    base_dir: &Path,
    repo: &str,
    token_type: &str,
    subject: &str,
    refresh_count: Arc<AtomicUsize>,
) -> Result<Arc<TranslatorConfig>, TestError> {
    refreshing_translator_with_expiration(RefreshRouteConfig {
        endpoint,
        base_dir,
        repo,
        token_type,
        subject,
        refresh_count,
        expiration: RefreshExpiration::Immediate,
    })
}

fn refreshing_translator_with_expiration(
    config: RefreshRouteConfig<'_>,
) -> Result<Arc<TranslatorConfig>, TestError> {
    let mut translator = TranslatorConfig::test_server_config(config.endpoint, config.base_dir)?;
    let refresh_url = format!(
        "{}/api/github/team/{}/xet-{}-token/main?subject={}",
        config.endpoint, config.repo, config.token_type, config.subject
    );
    translator.session.auth = AuthConfig::maybe_new(
        None,
        None,
        Some(Arc::new(RouteTokenRefresher::new(
            refresh_url,
            config.refresh_count,
            config.expiration,
        ))),
    );
    if translator.session.auth.is_none() {
        return Err(
            ServerE2eInvariantError::new("failed to install refreshing xet auth config").into(),
        );
    }
    Ok(Arc::new(translator))
}

async fn issue_xet_token(
    client: &Client,
    base_url: &str,
    repo: &str,
    token_type: &str,
    subject: &str,
) -> Result<XetCasTokenResponse, TestError> {
    issue_provider_xet_token(
        client,
        base_url,
        ProviderXetTokenRoute {
            provider: "github",
            owner: "team",
            repo,
            token_type,
            subject,
        },
    )
    .await
}

#[derive(Debug, Clone, Copy)]
struct ProviderXetTokenRoute<'route> {
    provider: &'route str,
    owner: &'route str,
    repo: &'route str,
    token_type: &'route str,
    subject: &'route str,
}

async fn issue_provider_xet_token(
    client: &Client,
    base_url: &str,
    route: ProviderXetTokenRoute<'_>,
) -> Result<XetCasTokenResponse, TestError> {
    let response = client
        .get(format!(
            "{base_url}/api/{}/{}/{}/xet-{}-token/main?subject={}",
            route.provider, route.owner, route.repo, route.token_type, route.subject
        ))
        .header("x-shardline-provider-key", "provider-bootstrap")
        .send()
        .await?
        .error_for_status()?
        .json::<XetCasTokenResponse>()
        .await?;
    Ok(response)
}

#[derive(Debug, Clone, Copy)]
struct ProviderNativeCase {
    kind: &'static str,
    owner: &'static str,
    repo: &'static str,
    subject: &'static str,
    seed: u8,
}

#[derive(Debug)]
struct GenericWebhookCase {
    event: &'static str,
    delivery: &'static str,
    body: Vec<u8>,
}

const fn all_provider_cases() -> [ProviderNativeCase; 4] {
    [
        ProviderNativeCase {
            kind: "github",
            owner: "team",
            repo: "assets-github",
            subject: "github-user-1",
            seed: 31,
        },
        ProviderNativeCase {
            kind: "gitlab",
            owner: "group",
            repo: "assets-gitlab",
            subject: "gitlab-user-1",
            seed: 47,
        },
        ProviderNativeCase {
            kind: "gitea",
            owner: "team",
            repo: "assets-gitea",
            subject: "gitea-user-1",
            seed: 63,
        },
        ProviderNativeCase {
            kind: "generic",
            owner: "team",
            repo: "assets-generic",
            subject: "generic-user-1",
            seed: 79,
        },
    ]
}

fn write_provider_config(root: &Path) -> Result<PathBuf, TestError> {
    let path = root.join("providers-native-xet.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "github",
                "integration_subject": "github-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets-a",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://github.example/team/assets-a.git",
                        "read_subjects": ["github-user-1"],
                        "write_subjects": ["github-user-1"]
                    },
                    {
                        "owner": "team",
                        "name": "assets-b",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://github.example/team/assets-b.git",
                        "read_subjects": ["github-user-2"],
                        "write_subjects": ["github-user-2"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn write_all_provider_config(root: &Path) -> Result<PathBuf, TestError> {
    let path = root.join("providers-all-native-xet.json");
    let providers = all_provider_cases()
        .iter()
        .map(|provider| {
            json!({
                "kind": provider.kind,
                "integration_subject": format!("{}-integration", provider.kind),
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": provider.owner,
                        "name": provider.repo,
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": format!(
                            "https://{}.example/{}/{}.git",
                            provider.kind, provider.owner, provider.repo
                        ),
                        "read_subjects": [provider.subject],
                        "write_subjects": [provider.subject]
                    }
                ]
            })
        })
        .collect::<Vec<_>>();
    let bytes = to_vec(&json!({ "providers": providers }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn generic_lifecycle_webhook_cases() -> Result<Vec<GenericWebhookCase>, TestError> {
    let repository = json!({
        "owner": "team",
        "name": "assets-generic"
    });
    let revision_body = to_vec(&json!({
        "kind": "revision_pushed",
        "repository": repository,
        "revision": "refs/heads/main"
    }))?;
    let access_body = to_vec(&json!({
        "kind": "access_changed",
        "repository": {
            "owner": "team",
            "name": "assets-generic"
        }
    }))?;

    Ok(vec![
        GenericWebhookCase {
            event: "revision_pushed",
            delivery: "lifecycle-revision-1",
            body: revision_body.clone(),
        },
        GenericWebhookCase {
            event: "access_changed",
            delivery: "lifecycle-access-1",
            body: access_body,
        },
        GenericWebhookCase {
            event: "revision_pushed",
            delivery: "lifecycle-revision-1",
            body: revision_body,
        },
    ])
}

async fn post_generic_webhook(
    client: &Client,
    base_url: &str,
    webhook: GenericWebhookCase,
) -> Result<(), TestError> {
    let signature = generic_webhook_signature(&webhook.body)?;
    client
        .post(format!("{base_url}/v1/providers/generic/webhooks"))
        .header("x-shardline-event", webhook.event)
        .header("x-shardline-delivery", webhook.delivery)
        .header("x-shardline-signature", signature)
        .body(webhook.body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn generic_webhook_signature(body: &[u8]) -> Result<String, TestError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret")?;
    mac.update(body);
    Ok(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

#[derive(Debug)]
struct RouteTokenRefresher {
    client: Client,
    refresh_url: String,
    refresh_count: Arc<AtomicUsize>,
    expiration: RefreshExpiration,
}

impl RouteTokenRefresher {
    fn new(
        refresh_url: String,
        refresh_count: Arc<AtomicUsize>,
        expiration: RefreshExpiration,
    ) -> Self {
        Self {
            client: Client::new(),
            refresh_url,
            refresh_count,
            expiration,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RefreshExpiration {
    Immediate,
    Issued,
}

#[async_trait]
impl TokenRefresher for RouteTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        self.refresh_count.fetch_add(1, Ordering::Relaxed);
        let issued = self
            .client
            .get(&self.refresh_url)
            .header("x-shardline-provider-key", "provider-bootstrap")
            .send()
            .await
            .map_err(AuthError::token_refresh_failure)?
            .error_for_status()
            .map_err(AuthError::token_refresh_failure)?
            .json::<XetCasTokenResponse>()
            .await
            .map_err(AuthError::token_refresh_failure)?;
        let local_expiration = match self.expiration {
            RefreshExpiration::Immediate => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(1_u64, |duration| duration.as_secs().saturating_add(1)),
            RefreshExpiration::Issued => issued.exp,
        };
        Ok((issued.access_token, local_expiration))
    }
}

fn seeded_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        let value = u8::try_from(index % 251).unwrap_or(0);
        bytes.push(value.wrapping_add(seed));
    }
    bytes
}

fn mutate_bytes(base: &[u8], start: usize, end: usize, value: u8) -> Result<Vec<u8>, TestError> {
    let mut bytes = base.to_vec();
    let region = bytes.get_mut(start..end);
    let Some(region) = region else {
        return Err(ServerE2eInvariantError::new("failed to select mutation region").into());
    };
    region.fill(value);
    Ok(bytes)
}

fn optional_s3_e2e_config() -> Option<S3ObjectStoreConfig> {
    let Ok(bucket) = var("SHARDLINE_S3_E2E_BUCKET") else {
        return None;
    };
    let region = var("SHARDLINE_S3_E2E_REGION").unwrap_or_else(|_error| "us-east-1".to_owned());
    let endpoint = var("SHARDLINE_S3_E2E_ENDPOINT").ok();
    let access_key_id = var("SHARDLINE_S3_E2E_ACCESS_KEY_ID").ok();
    let secret_access_key = var("SHARDLINE_S3_E2E_SECRET_ACCESS_KEY").ok();
    let session_token = var("SHARDLINE_S3_E2E_SESSION_TOKEN").ok();
    let configured_prefix = var("SHARDLINE_S3_E2E_KEY_PREFIX").ok();
    let allow_http = var("SHARDLINE_S3_E2E_ALLOW_HTTP")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "true" | "1" | "yes"));
    let unique_prefix = unique_s3_e2e_prefix(configured_prefix.as_deref());

    Some(
        S3ObjectStoreConfig::new(bucket, region)
            .with_endpoint(endpoint)
            .with_credentials(access_key_id, secret_access_key, session_token)
            .with_key_prefix(Some(&unique_prefix))
            .with_allow_http(allow_http),
    )
}

fn unique_s3_e2e_prefix(configured_prefix: Option<&str>) -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    configured_prefix.map_or_else(
        || format!("native-xet-e2e/{unix_nanos}"),
        |prefix| format!("{prefix}/native-xet-e2e/{unix_nanos}"),
    )
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
