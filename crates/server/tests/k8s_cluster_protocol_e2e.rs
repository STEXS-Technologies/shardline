mod support;

use std::{
    env::var,
    error::Error,
    fs::{read, write as write_file},
    io::Error as IoError,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{
        HeaderMap, Method, Response, StatusCode, Uri,
        header::{AUTHORIZATION, CONTENT_LENGTH, HOST},
    },
    routing::any,
    serve as serve_http,
};
use reqwest::{Client, StatusCode as ReqwestStatusCode};
use serde_json::{from_slice, to_vec};
use shardline_server::{
    FileReconstructionResponse, FileReconstructionV2Response, ObjectStorageAdapter, ServerConfig,
    ServerError, ServerStatsResponse, XetCasTokenResponse, apply_database_migrations,
    serve_with_listener,
};
use shardline_storage::{
    ObjectPrefix, ObjectStore, S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError,
};
use shardline_test_support::DockerLocalStack;
use sqlx::{PgPool, postgres::PgPoolOptions, query_as, query_scalar};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, task::JoinHandle, time::sleep};
use xet_client::chunk_cache::{CacheConfig, get_cache};
use xet_data::processing::{
    FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo,
    configurations::TranslatorConfig,
};

type TestError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone)]
struct ClusterConfig {
    base_url: String,
    metrics_url: Option<String>,
    metrics_token: Option<String>,
    provider: String,
    owner: String,
    repo: String,
    subject: String,
    provider_key: String,
    postgres_url: String,
    redis_url: String,
    s3_config: S3ObjectStoreConfig,
}

struct LocalClusterRuntime {
    _services: DockerLocalStack,
    _storage: tempfile::TempDir,
    config: ClusterConfig,
    server: JoinHandle<Result<(), ServerError>>,
}

impl Drop for LocalClusterRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl ClusterConfig {
    fn from_env() -> Option<Self> {
        let base_url = var("SHARDLINE_K8S_E2E_BASE_URL").ok()?;
        let bucket = var("SHARDLINE_K8S_E2E_S3_BUCKET").ok()?;
        let region =
            var("SHARDLINE_K8S_E2E_S3_REGION").unwrap_or_else(|_error| "us-east-1".to_owned());
        let endpoint = var("SHARDLINE_K8S_E2E_S3_ENDPOINT").ok();
        let access_key_id = var("SHARDLINE_K8S_E2E_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = var("SHARDLINE_K8S_E2E_S3_SECRET_ACCESS_KEY").ok();
        let allow_http = var("SHARDLINE_K8S_E2E_S3_ALLOW_HTTP")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "true" | "1" | "yes"));

        Some(Self {
            base_url,
            metrics_url: var("SHARDLINE_K8S_E2E_METRICS_URL").ok(),
            metrics_token: var("SHARDLINE_K8S_E2E_METRICS_TOKEN").ok(),
            provider: var("SHARDLINE_K8S_E2E_PROVIDER")
                .unwrap_or_else(|_error| "generic".to_owned()),
            owner: var("SHARDLINE_K8S_E2E_OWNER").unwrap_or_else(|_error| "team".to_owned()),
            repo: var("SHARDLINE_K8S_E2E_REPO").unwrap_or_else(|_error| "assets".to_owned()),
            subject: var("SHARDLINE_K8S_E2E_SUBJECT")
                .unwrap_or_else(|_error| "operator".to_owned()),
            provider_key: var("SHARDLINE_K8S_E2E_PROVIDER_KEY")
                .unwrap_or_else(|_error| "local-provider-bootstrap-key".to_owned()),
            postgres_url: var("SHARDLINE_K8S_E2E_POSTGRES_URL").ok()?,
            redis_url: var("SHARDLINE_K8S_E2E_REDIS_URL").ok()?,
            s3_config: S3ObjectStoreConfig::new(bucket, region)
                .with_endpoint(endpoint)
                .with_credentials(access_key_id, secret_access_key, None)
                .with_allow_http(allow_http),
        })
    }
}

async fn start_local_cluster_runtime() -> Result<Option<LocalClusterRuntime>, TestError> {
    let Some(services) = DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .with_redis()
        .start()?
    else {
        return Ok(None);
    };

    let storage = tempfile::tempdir()?;
    let provider_config = write_generic_provider_config(storage.path())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let postgres_url = services
        .postgres_url()
        .ok_or_else(|| ServerE2eInvariantError::new("postgres url was unavailable"))?;
    let migration_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&postgres_url)
        .await?;
    apply_database_migrations(&migration_pool).await?;
    migration_pool.close().await;
    let redis_url = services
        .redis_url()
        .ok_or_else(|| ServerE2eInvariantError::new("redis url was unavailable"))?;
    let key_prefix = services.unique_s3_key_prefix("k8s-cluster-protocol-e2e");
    let s3_config = services
        .s3_config_with_prefix(Some(&key_prefix))
        .ok_or_else(|| ServerE2eInvariantError::new("minio s3 config was unavailable"))?;
    let metrics_token = b"metrics-token".to_vec();
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).ok_or("chunk size")?,
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config.clone()))
    .with_index_postgres_url(postgres_url.clone())?
    .with_reconstruction_cache_redis(redis_url.clone(), NonZeroU64::new(30).ok_or("cache ttl")?)?
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_metrics_token(metrics_token.clone())?
    .with_provider_runtime(
        provider_config,
        b"local-provider-bootstrap-key".to_vec(),
        "local-cluster-e2e".to_owned(),
        NonZeroU64::new(300).ok_or("provider token ttl")?,
    )?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    let client = Client::new();
    wait_for_health(&client, &base_url).await?;

    Ok(Some(LocalClusterRuntime {
        _services: services,
        _storage: storage,
        config: ClusterConfig {
            base_url: base_url.clone(),
            metrics_url: Some(format!("{base_url}/metrics")),
            metrics_token: Some(String::from_utf8(metrics_token)?),
            provider: "generic".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            subject: "generic-user-1".to_owned(),
            provider_key: "local-provider-bootstrap-key".to_owned(),
            postgres_url,
            redis_url,
            s3_config,
        },
        server,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageInventory {
    total_objects: u64,
    total_bytes: u64,
    chunk_objects: u64,
    chunk_bytes: u64,
    xorb_objects: u64,
    xorb_bytes: u64,
    shard_objects: u64,
    shard_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataInventory {
    latest_records: i64,
    version_records: i64,
    reconstructions: i64,
    xorbs: i64,
    dedupe_shards: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredVersionIdentity {
    file_id: String,
    content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheInventory {
    dbsize: u64,
    shardline_keys: u64,
}

#[derive(Debug, Clone, Copy)]
struct AssetScenario<'scenario> {
    logical_name: &'scenario str,
    file_size_bytes: usize,
    seed: u8,
    mutation_start: usize,
    mutation_end: usize,
    mutation_value: u8,
}

#[derive(Debug, Clone)]
struct AssetRun {
    scenario: AssetScenario<'static>,
    first_info: XetFileInfo,
    second_info: XetFileInfo,
    first_bytes: Vec<u8>,
    second_bytes: Vec<u8>,
    first_metrics: xet_data::deduplication::DeduplicationMetrics,
    second_metrics: xet_data::deduplication::DeduplicationMetrics,
    latest_version: StoredVersionIdentity,
    first_chunk_delta: u64,
    second_chunk_delta: u64,
    first_total_byte_delta: u64,
    second_total_byte_delta: u64,
    first_chunk_byte_delta: u64,
    second_chunk_byte_delta: u64,
}

#[derive(Debug, Clone, Copy)]
struct InventorySnapshot {
    storage: StorageInventory,
    metadata: MetadataInventory,
}

#[derive(Debug)]
struct AssetHarness<'harness> {
    upload_translator: &'harness Arc<TranslatorConfig>,
    object_store: &'harness S3ObjectStore,
    postgres_pool: &'harness PgPool,
    run_nonce: u64,
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
    access_token: String,
    client: Client,
    observations: Arc<Mutex<Vec<ProxyObservation>>>,
    proxy_base_url: String,
    upstream_base_url: String,
}

#[derive(Debug, Clone, Copy)]
struct ProviderRoute<'route> {
    provider: &'route str,
    owner: &'route str,
    repo: &'route str,
    subject: &'route str,
    token_type: &'route str,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k8s_cluster_native_xet_sparse_mutation_flow_uses_s3_postgres_and_garnet() {
    let result = async {
        let local_runtime = if let Some(config) = ClusterConfig::from_env() {
            (config, None)
        } else {
            let Some(runtime) = start_local_cluster_runtime().await? else {
                return Ok(());
            };
            (runtime.config.clone(), Some(runtime))
        };
        let (config, _runtime) = local_runtime;
        exercise_k8s_cluster_native_xet_sparse_mutation_flow(config).await
    }
    .await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "k8s cluster protocol e2e failed: {error:?}");
}

async fn exercise_k8s_cluster_native_xet_sparse_mutation_flow(
    config: ClusterConfig,
) -> Result<(), TestError> {
    let client = Client::new();
    wait_for_health(&client, &config.base_url).await?;

    let postgres_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.postgres_url)
        .await?;
    let object_store = S3ObjectStore::new(config.s3_config.clone())?;

    let before_storage = storage_inventory(&object_store)?;
    let before_metadata = metadata_inventory(&postgres_pool).await?;
    let before_cache = cache_inventory(&config.redis_url).await?;

    let write_token = issue_provider_xet_token(
        &client,
        &config.base_url,
        &config.provider_key,
        ProviderRoute {
            provider: &config.provider,
            owner: &config.owner,
            repo: &config.repo,
            subject: &config.subject,
            token_type: "write",
        },
    )
    .await?;
    let read_token = issue_provider_xet_token(
        &client,
        &config.base_url,
        &config.provider_key,
        ProviderRoute {
            provider: &config.provider,
            owner: &config.owner,
            repo: &config.repo,
            subject: &config.subject,
            token_type: "read",
        },
    )
    .await?;

    let upload_root = tempfile::tempdir()?;
    let download_root = tempfile::tempdir()?;
    let run_nonce = unique_nonce()?;
    let upload_translator =
        authenticated_translator(&config.base_url, upload_root.path(), &write_token)?;
    let asset_harness = AssetHarness {
        upload_translator: &upload_translator,
        object_store: &object_store,
        postgres_pool: &postgres_pool,
        run_nonce,
    };
    let scenarios = [
        AssetScenario {
            logical_name: "atlas",
            file_size_bytes: 768 * 1024,
            seed: 0x17,
            mutation_start: 256 * 1024,
            mutation_end: 288 * 1024,
            mutation_value: 0x5a,
        },
        AssetScenario {
            logical_name: "citadel",
            file_size_bytes: 896 * 1024,
            seed: 0x39,
            mutation_start: 320 * 1024,
            mutation_end: 352 * 1024,
            mutation_value: 0x7c,
        },
        AssetScenario {
            logical_name: "delta",
            file_size_bytes: 1_024 * 1024,
            seed: 0x63,
            mutation_start: 448 * 1024,
            mutation_end: 480 * 1024,
            mutation_value: 0x91,
        },
    ];
    let mut previous_snapshot = InventorySnapshot {
        storage: before_storage,
        metadata: before_metadata,
    };
    let mut asset_runs = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let asset_run =
            exercise_asset_scenario(&asset_harness, scenario, previous_snapshot).await?;
        previous_snapshot = InventorySnapshot {
            storage: storage_inventory(&object_store)?,
            metadata: metadata_inventory(&postgres_pool).await?,
        };
        asset_runs.push(asset_run);
    }
    let after_second_storage = previous_snapshot.storage;
    let after_second_metadata = previous_snapshot.metadata;
    let stats_after_second = read_server_stats(&client, &config.base_url, &read_token).await?;

    assert_eq!(asset_runs.len(), 3);
    assert!(stats_after_second.chunks > 0);
    assert!(stats_after_second.files >= u64::try_from(asset_runs.len())?);
    assert!(after_second_storage.xorb_objects > before_storage.xorb_objects);
    assert!(after_second_storage.shard_objects > before_storage.shard_objects);
    assert!(after_second_storage.xorb_bytes > before_storage.xorb_bytes);
    assert!(after_second_storage.shard_bytes > before_storage.shard_bytes);
    assert!(after_second_metadata.version_records > before_metadata.version_records);
    assert!(after_second_metadata.latest_records >= before_metadata.latest_records);
    assert!(after_second_metadata.reconstructions >= before_metadata.reconstructions);
    assert!(after_second_metadata.xorbs >= before_metadata.xorbs);
    assert!(after_second_metadata.dedupe_shards >= before_metadata.dedupe_shards);
    for asset_run in &asset_runs {
        assert_ne!(asset_run.first_info.hash(), asset_run.second_info.hash());
        assert!(asset_run.first_metrics.total_bytes > 0);
        assert!(asset_run.second_metrics.deduped_chunks > 0);
        assert!(asset_run.second_metrics.new_bytes < asset_run.second_metrics.total_bytes);
        assert!(asset_run.second_metrics.new_bytes < asset_run.first_metrics.new_bytes);
        assert!(asset_run.first_chunk_delta > 0);
        assert!(asset_run.second_chunk_delta > 0);
        assert!(asset_run.second_chunk_delta < asset_run.first_chunk_delta);
        assert!(asset_run.first_total_byte_delta > 0);
        assert!(asset_run.second_total_byte_delta > 0);
        assert!(asset_run.second_total_byte_delta < asset_run.first_total_byte_delta);
        assert!(asset_run.first_chunk_byte_delta > 0);
        assert!(asset_run.second_chunk_byte_delta > 0);
        assert!(asset_run.second_chunk_byte_delta < asset_run.first_chunk_byte_delta);
    }

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_base_url = format!("http://{proxy_addr}");
    let proxy_state = ProxyState {
        access_token: read_token.access_token.clone(),
        client: client.clone(),
        observations: Arc::new(Mutex::new(Vec::new())),
        proxy_base_url: proxy_base_url.clone(),
        upstream_base_url: config.base_url.clone(),
    };
    let proxy_router = Router::new()
        .fallback(any(handle_proxy))
        .with_state(proxy_state.clone());
    let proxy_server = spawn(async move { serve_http(proxy_listener, proxy_router).await });

    let proxy_download_translator =
        authenticated_translator(&proxy_base_url, download_root.path(), &read_token)?;
    let chunk_cache = get_cache(&CacheConfig {
        cache_directory: download_root.path().join("chunk-cache"),
        cache_size: 64 * 1024 * 1024,
    })?;
    let download_session =
        FileDownloadSession::new(proxy_download_translator, Some(chunk_cache)).await?;

    for asset_run in &asset_runs {
        let first_output = download_root
            .path()
            .join(format!("{}-first.out", asset_run.scenario.logical_name));
        let second_output = download_root
            .path()
            .join(format!("{}-second.out", asset_run.scenario.logical_name));
        let (_first_download_id, first_downloaded) = download_session
            .download_file(&asset_run.first_info, &first_output)
            .await?;
        assert_eq!(
            first_downloaded,
            u64::try_from(asset_run.first_bytes.len())?
        );
        assert_eq!(read(&first_output)?, asset_run.first_bytes);

        let cold_observations = aggregate_proxy_observations(&proxy_state.observations)?;
        clear_proxy_observations(&proxy_state.observations)?;

        let (_second_download_id, second_downloaded) = download_session
            .download_file(&asset_run.second_info, &second_output)
            .await?;
        assert_eq!(
            second_downloaded,
            u64::try_from(asset_run.second_bytes.len())?
        );
        assert_eq!(read(&second_output)?, asset_run.second_bytes);

        let warm_observations = aggregate_proxy_observations(&proxy_state.observations)?;
        let mutated_window_bytes = u64::try_from(
            asset_run
                .scenario
                .mutation_end
                .checked_sub(asset_run.scenario.mutation_start)
                .ok_or_else(|| ServerE2eInvariantError::new("mutation window underflowed"))?,
        )?;
        assert!(cold_observations.transfer_bytes > 0);
        assert!(warm_observations.transfer_bytes > 0);
        assert!(
            warm_observations.transfer_bytes < u64::try_from(asset_run.second_bytes.len())?,
            "warm native-xet transfer downloaded the full file: asset={}, transfer={}, file={}",
            asset_run.scenario.logical_name,
            warm_observations.transfer_bytes,
            asset_run.second_bytes.len()
        );
        assert!(
            warm_observations.transfer_bytes <= mutated_window_bytes.saturating_mul(8),
            "warm native-xet transfer exceeded sparse-update budget: asset={}, warm={}, mutation_window={}",
            asset_run.scenario.logical_name,
            warm_observations.transfer_bytes,
            mutated_window_bytes
        );
        clear_proxy_observations(&proxy_state.observations)?;
    }

    let cache_before_reconstruction = cache_inventory(&config.redis_url).await?;
    for asset_run in &asset_runs {
        let first_reconstruction = read_reconstruction_response(
            &client,
            &config.base_url,
            &read_token.access_token,
            &asset_run.latest_version.file_id,
            Some(&asset_run.latest_version.content_hash),
        )
        .await?;
        assert!(!first_reconstruction.terms.is_empty());
        assert!(!first_reconstruction.fetch_info.is_empty());
        let second_reconstruction = read_reconstruction_response(
            &client,
            &config.base_url,
            &read_token.access_token,
            &asset_run.latest_version.file_id,
            Some(&asset_run.latest_version.content_hash),
        )
        .await?;
        assert_eq!(first_reconstruction.terms, second_reconstruction.terms);
        assert_eq!(
            first_reconstruction.fetch_info,
            second_reconstruction.fetch_info
        );
    }
    let cache_after_reconstruction = cache_inventory(&config.redis_url).await?;
    assert!(cache_after_reconstruction.dbsize >= before_cache.dbsize);
    assert!(cache_after_reconstruction.dbsize >= cache_before_reconstruction.dbsize);
    assert!(cache_after_reconstruction.shardline_keys > before_cache.shardline_keys);
    assert!(cache_after_reconstruction.shardline_keys > cache_before_reconstruction.shardline_keys);

    if let Some(metrics_url) = config.metrics_url.as_deref() {
        if config.metrics_token.is_some() {
            let status = client.get(metrics_url).send().await?.status();
            assert_eq!(status, ReqwestStatusCode::UNAUTHORIZED);
        }

        let mut request = client.get(metrics_url);
        if let Some(metrics_token) = config.metrics_token.as_deref() {
            request = request.bearer_auth(metrics_token);
        }
        let metrics = request.send().await?.error_for_status()?.text().await?;
        assert!(metrics.contains("shardline_up 1"));
        assert!(metrics.contains("shardline_server_info"));
        assert!(metrics.contains("shardline_chunk_size_bytes"));
        assert!(metrics.contains("shardline_provider_tokens_enabled 1"));
        if config.metrics_token.is_some() {
            assert!(metrics.contains("shardline_metrics_auth_enabled 1"));
        }
    }

    proxy_server.abort();
    Ok(())
}

async fn exercise_asset_scenario(
    harness: &AssetHarness<'_>,
    scenario: AssetScenario<'static>,
    previous_snapshot: InventorySnapshot,
) -> Result<AssetRun, TestError> {
    let first_bytes =
        seeded_unique_bytes(scenario.file_size_bytes, scenario.seed, harness.run_nonce)?;
    let second_bytes = mutate_bytes(
        &first_bytes,
        scenario.mutation_start,
        scenario.mutation_end,
        scenario.mutation_value,
    )?;

    let (first_info, first_metrics) = upload_bytes(
        Arc::clone(harness.upload_translator),
        &format!("{}-first.bin", scenario.logical_name),
        &first_bytes,
    )
    .await?;
    let after_first_storage = storage_inventory(harness.object_store)?;
    let after_first_metadata = metadata_inventory(harness.postgres_pool).await?;

    let (second_info, second_metrics) = upload_bytes(
        Arc::clone(harness.upload_translator),
        &format!("{}-second.bin", scenario.logical_name),
        &second_bytes,
    )
    .await?;
    let after_second_storage = storage_inventory(harness.object_store)?;
    let after_second_metadata = metadata_inventory(harness.postgres_pool).await?;
    let latest_version = latest_version_identity(harness.postgres_pool).await?;

    assert!(after_first_metadata.version_records > previous_snapshot.metadata.version_records);
    assert!(after_second_metadata.version_records > after_first_metadata.version_records);
    assert!(after_second_metadata.reconstructions >= after_first_metadata.reconstructions);
    assert!(after_second_metadata.xorbs >= after_first_metadata.xorbs);
    assert!(after_second_metadata.dedupe_shards >= after_first_metadata.dedupe_shards);
    assert!(after_second_storage.xorb_objects > after_first_storage.xorb_objects);
    assert!(after_second_storage.shard_objects > after_first_storage.shard_objects);
    assert!(after_second_storage.xorb_bytes > after_first_storage.xorb_bytes);
    assert!(after_second_storage.shard_bytes > after_first_storage.shard_bytes);

    Ok(AssetRun {
        scenario,
        first_info,
        second_info,
        first_bytes,
        second_bytes,
        first_metrics,
        second_metrics,
        latest_version,
        first_chunk_delta: checked_u64_delta(
            after_first_storage.chunk_objects,
            previous_snapshot.storage.chunk_objects,
            "first upload chunk object delta",
        )?,
        second_chunk_delta: checked_u64_delta(
            after_second_storage.chunk_objects,
            after_first_storage.chunk_objects,
            "second upload chunk object delta",
        )?,
        first_total_byte_delta: checked_u64_delta(
            after_first_storage.total_bytes,
            previous_snapshot.storage.total_bytes,
            "first upload total object byte delta",
        )?,
        second_total_byte_delta: checked_u64_delta(
            after_second_storage.total_bytes,
            after_first_storage.total_bytes,
            "second upload total object byte delta",
        )?,
        first_chunk_byte_delta: checked_u64_delta(
            after_first_storage.chunk_bytes,
            previous_snapshot.storage.chunk_bytes,
            "first upload chunk byte delta",
        )?,
        second_chunk_byte_delta: checked_u64_delta(
            after_second_storage.chunk_bytes,
            after_first_storage.chunk_bytes,
            "second upload chunk byte delta",
        )?,
    })
}

fn authenticated_translator(
    endpoint: &str,
    base_dir: &Path,
    token: &XetCasTokenResponse,
) -> Result<Arc<TranslatorConfig>, TestError> {
    let mut translator = TranslatorConfig::test_server_config(endpoint, base_dir)?;
    translator.session.auth = xet_client::cas_client::auth::AuthConfig::maybe_new(
        Some(token.access_token.clone()),
        Some(token.exp),
        None,
    );
    if translator.session.auth.is_none() {
        return Err(ServerE2eInvariantError::new("failed to install xet auth config").into());
    }
    Ok(Arc::new(translator))
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

async fn issue_provider_xet_token(
    client: &Client,
    base_url: &str,
    provider_key: &str,
    route: ProviderRoute<'_>,
) -> Result<XetCasTokenResponse, TestError> {
    Ok(client
        .get(format!(
            "{base_url}/api/{}/{}/{}/xet-{}-token/main?subject={}",
            route.provider, route.owner, route.repo, route.token_type, route.subject
        ))
        .header("x-shardline-provider-key", provider_key)
        .send()
        .await?
        .error_for_status()?
        .json::<XetCasTokenResponse>()
        .await?)
}

async fn read_server_stats(
    client: &Client,
    base_url: &str,
    read_token: &XetCasTokenResponse,
) -> Result<ServerStatsResponse, TestError> {
    Ok(client
        .get(format!("{base_url}/v1/stats"))
        .bearer_auth(&read_token.access_token)
        .send()
        .await?
        .error_for_status()?
        .json::<ServerStatsResponse>()
        .await?)
}

fn storage_inventory(store: &S3ObjectStore) -> Result<StorageInventory, TestError> {
    let prefix = ObjectPrefix::parse("")?;
    let mut inventory = StorageInventory {
        total_objects: 0,
        total_bytes: 0,
        chunk_objects: 0,
        chunk_bytes: 0,
        xorb_objects: 0,
        xorb_bytes: 0,
        shard_objects: 0,
        shard_bytes: 0,
    };
    store.visit_prefix(&prefix, |metadata| {
        inventory.total_objects =
            checked_add_u64(inventory.total_objects, 1, "total object count overflowed")?;
        inventory.total_bytes = checked_add_u64(
            inventory.total_bytes,
            metadata.length(),
            "total object bytes overflowed",
        )?;
        if is_chunk_object_key(metadata.key().as_str()) {
            inventory.chunk_objects =
                checked_add_u64(inventory.chunk_objects, 1, "chunk object count overflowed")?;
            inventory.chunk_bytes = checked_add_u64(
                inventory.chunk_bytes,
                metadata.length(),
                "chunk object bytes overflowed",
            )?;
        }
        if is_xorb_object_key(metadata.key().as_str()) {
            inventory.xorb_objects =
                checked_add_u64(inventory.xorb_objects, 1, "xorb object count overflowed")?;
            inventory.xorb_bytes = checked_add_u64(
                inventory.xorb_bytes,
                metadata.length(),
                "xorb object bytes overflowed",
            )?;
        }
        if is_shard_object_key(metadata.key().as_str()) {
            inventory.shard_objects =
                checked_add_u64(inventory.shard_objects, 1, "shard object count overflowed")?;
            inventory.shard_bytes = checked_add_u64(
                inventory.shard_bytes,
                metadata.length(),
                "shard object bytes overflowed",
            )?;
        }
        Ok::<(), S3ObjectStoreError>(())
    })?;
    Ok(inventory)
}

async fn metadata_inventory(pool: &PgPool) -> Result<MetadataInventory, TestError> {
    Ok(MetadataInventory {
        latest_records: query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_file_records WHERE record_kind = 'latest'",
        )
        .fetch_one(pool)
        .await?,
        version_records: query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_file_records WHERE record_kind = 'version'",
        )
        .fetch_one(pool)
        .await?,
        reconstructions: query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_file_reconstructions",
        )
        .fetch_one(pool)
        .await?,
        xorbs: query_scalar::<_, i64>("SELECT COUNT(*) FROM shardline_stored_objects")
            .fetch_one(pool)
            .await?,
        dedupe_shards: query_scalar::<_, i64>("SELECT COUNT(*) FROM shardline_dedupe_shards")
            .fetch_one(pool)
            .await?,
    })
}

async fn latest_version_identity(pool: &PgPool) -> Result<StoredVersionIdentity, TestError> {
    let row = query_as::<_, (String, String)>(
        "SELECT file_id, content_hash
         FROM shardline_file_records
         WHERE record_kind = 'version'
         ORDER BY updated_at DESC, record_key DESC
         LIMIT 1",
    )
    .fetch_one(pool)
    .await?;
    Ok(StoredVersionIdentity {
        file_id: row.0,
        content_hash: row.1,
    })
}

fn write_generic_provider_config(root: &Path) -> Result<std::path::PathBuf, TestError> {
    let path = root.join("providers-generic-k8s-e2e.json");
    let bytes = to_vec(&serde_json::json!({
        "providers": [
            {
                "kind": "generic",
                "integration_subject": "generic-bridge",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://forge.example/team/assets.git",
                        "read_subjects": ["generic-user-1"],
                        "write_subjects": ["generic-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

async fn cache_inventory(redis_url: &str) -> Result<CacheInventory, TestError> {
    let client = redis::Client::open(redis_url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let dbsize: u64 = redis::cmd("DBSIZE").query_async(&mut connection).await?;
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("shardline:reconstruction:v1:*")
        .query_async(&mut connection)
        .await?;
    Ok(CacheInventory {
        dbsize,
        shardline_keys: u64::try_from(keys.len())?,
    })
}

async fn read_reconstruction_response(
    client: &Client,
    base_url: &str,
    token: &str,
    file_id: &str,
    content_hash: Option<&str>,
) -> Result<FileReconstructionResponse, TestError> {
    let mut request = client
        .get(format!("{base_url}/v1/reconstructions/{file_id}"))
        .bearer_auth(token);
    if let Some(content_hash) = content_hash {
        request = request.query(&[("content_hash", content_hash)]);
    }
    Ok(request
        .send()
        .await?
        .error_for_status()?
        .json::<FileReconstructionResponse>()
        .await?)
}

async fn handle_proxy(
    State(state): State<ProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, (StatusCode, String)> {
    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| uri.path().to_owned(), ToString::to_string);
    let upstream_url = format!("{}{}", state.upstream_base_url, path_and_query);
    let mut request = state.client.request(method, upstream_url).body(body);

    for (name, value) in &headers {
        if *name != HOST && *name != CONTENT_LENGTH {
            request = request.header(name, value);
        }
    }
    if !headers.contains_key(AUTHORIZATION)
        && (path_and_query.starts_with("/transfer/xorb/")
            || path_and_query.starts_with("/v1/chunks/"))
    {
        request = request.bearer_auth(&state.access_token);
    }

    let response = request.send().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("cluster proxy request failed: {error}"),
        )
    })?;
    let status = response.status();
    let response_headers = response.headers().clone();
    let original_response_bytes = response.bytes().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("cluster proxy body read failed: {error}"),
        )
    })?;
    let response_bytes = if status.is_success()
        && (path_and_query.starts_with("/v1/reconstructions/")
            || path_and_query.starts_with("/v2/reconstructions/"))
    {
        rewrite_reconstruction_urls(
            &original_response_bytes,
            &path_and_query,
            &state.upstream_base_url,
            &state.proxy_base_url,
        )?
    } else {
        original_response_bytes.to_vec()
    };
    let transfer_bytes = if path_and_query.starts_with("/transfer/xorb/")
        || path_and_query.starts_with("/v1/chunks/")
    {
        u64::try_from(response_bytes.len()).map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("cluster proxy length overflowed: {error}"),
            )
        })?
    } else {
        0
    };

    {
        let mut observations = state.observations.lock().map_err(|_error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cluster proxy observation lock poisoned".to_owned(),
            )
        })?;
        observations.push(ProxyObservation {
            path_and_query: path_and_query.clone(),
            status: status.as_u16(),
            response_bytes: u64::try_from(response_bytes.len()).map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("cluster proxy response length overflowed: {error}"),
                )
            })?,
            transfer_bytes,
        });
    }

    let mut proxied = Response::builder().status(status);
    for (name, value) in &response_headers {
        if *name != CONTENT_LENGTH {
            proxied = proxied.header(name, value);
        }
    }
    proxied = proxied.header(CONTENT_LENGTH, response_bytes.len().to_string());
    proxied.body(Body::from(response_bytes)).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cluster proxy response build failed: {error}"),
        )
    })
}

fn aggregate_proxy_observations(
    observations: &Arc<Mutex<Vec<ProxyObservation>>>,
) -> Result<ProxyObservation, TestError> {
    let observations = observations.lock().map_err(|_error| {
        ServerE2eInvariantError::new("cluster proxy observation lock poisoned")
    })?;
    let mut combined = ProxyObservation {
        path_and_query: String::new(),
        status: 200,
        response_bytes: 0,
        transfer_bytes: 0,
    };
    for observation in observations.iter() {
        if observation.status >= 400
            && observation.status != StatusCode::RANGE_NOT_SATISFIABLE.as_u16()
        {
            return Err(ServerE2eInvariantError::new(format!(
                "cluster proxy observed failed upstream request: {} {}",
                observation.status, observation.path_and_query
            ))
            .into());
        }
        combined.response_bytes = combined
            .response_bytes
            .checked_add(observation.response_bytes)
            .ok_or_else(|| ServerE2eInvariantError::new("proxy response byte total overflowed"))?;
        combined.transfer_bytes = combined
            .transfer_bytes
            .checked_add(observation.transfer_bytes)
            .ok_or_else(|| ServerE2eInvariantError::new("proxy transfer byte total overflowed"))?;
    }
    Ok(combined)
}

fn clear_proxy_observations(
    observations: &Arc<Mutex<Vec<ProxyObservation>>>,
) -> Result<(), TestError> {
    let mut observations = observations.lock().map_err(|_error| {
        ServerE2eInvariantError::new("cluster proxy observation lock poisoned")
    })?;
    observations.clear();
    Ok(())
}

fn rewrite_reconstruction_urls(
    response_bytes: &Bytes,
    path_and_query: &str,
    upstream_base_url: &str,
    proxy_base_url: &str,
) -> Result<Vec<u8>, (StatusCode, String)> {
    if path_and_query.starts_with("/v2/reconstructions/") {
        let mut response =
            from_slice::<FileReconstructionV2Response>(response_bytes).map_err(|error| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("cluster proxy reconstruction v2 decode failed: {error}"),
                )
            })?;
        for fetches in response.xorbs.values_mut() {
            for fetch in fetches {
                if fetch.url.starts_with(upstream_base_url) {
                    fetch.url = fetch.url.replacen(upstream_base_url, proxy_base_url, 1);
                }
            }
        }
        return to_vec(&response).map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("cluster proxy reconstruction v2 encode failed: {error}"),
            )
        });
    }

    let mut response =
        from_slice::<FileReconstructionResponse>(response_bytes).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("cluster proxy reconstruction v1 decode failed: {error}"),
            )
        })?;
    for fetches in response.fetch_info.values_mut() {
        for fetch in fetches {
            if fetch.url.starts_with(upstream_base_url) {
                fetch.url = fetch.url.replacen(upstream_base_url, proxy_base_url, 1);
            }
        }
    }
    to_vec(&response).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cluster proxy reconstruction encode failed: {error}"),
        )
    })
}

fn seeded_unique_bytes(len: usize, seed: u8, nonce: u64) -> Result<Vec<u8>, TestError> {
    let mut bytes = Vec::with_capacity(len);
    let mut counter = 0_u64;
    while bytes.len() < len {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&[seed]);
        hasher.update(&nonce.to_le_bytes());
        hasher.update(&counter.to_le_bytes());
        let block = hasher.finalize();
        bytes.extend_from_slice(block.as_bytes());
        counter = counter
            .checked_add(1)
            .ok_or_else(|| ServerE2eInvariantError::new("seeded byte counter overflowed"))?;
    }
    bytes.truncate(len);
    Ok(bytes)
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

async fn wait_for_health(client: &Client, base_url: &str) -> Result<(), TestError> {
    for _attempt in 0..120 {
        if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(ServerE2eInvariantError::new("cluster did not become healthy").into())
}

fn is_chunk_object_key(key: &str) -> bool {
    let mut segments = key.split('/');
    let Some(prefix) = segments.next() else {
        return false;
    };
    let Some(hash) = segments.next() else {
        return false;
    };
    segments.next().is_none()
        && prefix.len() == 2
        && prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
        && hash.len() == 64
        && hash.starts_with(prefix)
        && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_xorb_object_key(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some("xorbs"), Some("default"), Some(prefix), Some(file_name), None)
            if prefix.len() == 2
                && prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
                && file_name.len() == 69
                && file_name.ends_with(".xorb")
                && file_name[..64].starts_with(prefix)
                && file_name[..64].bytes().all(|byte| byte.is_ascii_hexdigit())
    )
}

fn is_shard_object_key(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some("shards"), Some(prefix), Some(file_name), None)
            if prefix.len() == 2
                && prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
                && file_name.len() == 70
                && file_name.ends_with(".shard")
                && file_name[..64].starts_with(prefix)
                && file_name[..64].bytes().all(|byte| byte.is_ascii_hexdigit())
    )
}

fn checked_u64_delta(current: u64, previous: u64, label: &str) -> Result<u64, TestError> {
    current
        .checked_sub(previous)
        .ok_or_else(|| ServerE2eInvariantError::new(format!("{label} underflowed")).into())
}

fn checked_add_u64(current: u64, value: u64, label: &str) -> Result<u64, IoError> {
    current
        .checked_add(value)
        .ok_or_else(|| ServerE2eInvariantError::new(label).into())
}

fn unique_nonce() -> Result<u64, TestError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ServerE2eInvariantError::new(format!("system clock error: {error}")))?
        .as_nanos();
    let bounded = nanos
        .checked_rem(u128::from(u64::MAX))
        .ok_or_else(|| ServerE2eInvariantError::new("nonce remainder overflowed"))?;
    Ok(u64::try_from(bounded)?)
}
