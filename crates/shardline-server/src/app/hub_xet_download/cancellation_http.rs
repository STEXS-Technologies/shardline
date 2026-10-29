//! Real TCP disconnect while a later container is being prepared.
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{
    Router,
    routing::{get, post},
};
use sha2::{Digest, Sha256};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::{
    AppState, LocalBackend, ProtocolMetrics, ReconstructionCacheService, ServerBackend,
    ServerConfig, ServerRole, TransferLimiter,
    admission::{BoundedPool, ExecutionPools, WeightedAdmission},
    auth::ServerAuth,
    object_store::{ServerObjectStore, set_before_local_object_read_hook},
};

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnected_multicontainer_http_download_releases_work_and_retries_verified() {
    exercise_disconnected_download(false).await;
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn disconnected_remote_native_download_stops_before_next_source_and_retries_verified() {
    exercise_disconnected_download(true).await;
}

async fn exercise_disconnected_download(remote: bool) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    #[cfg(feature = "docker")]
    let remote_fixture = if remote {
        use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
        let stack = tokio::task::spawn_blocking(|| {
            DockerLocalStack::builder()
                .with_postgres()
                .with_minio()
                .start()
        })
        .await
        .unwrap()
        .unwrap()
        .expect("Docker is required");
        let database = stack.postgres_url().expect("PostgreSQL is required");
        let owner = sqlx::PgPool::connect(&database).await.unwrap();
        crate::apply_database_migrations(&owner).await.unwrap();
        owner.close().await;
        let raw = stack
            .s3_raw_config(Some("native-download-disconnect"))
            .unwrap();
        let proxy = S3FaultProxy::start_disarmed(
            raw.endpoint.as_deref().unwrap(),
            S3ProxyFault::DelaySuccessfulGetBody(Duration::from_secs(10)),
        )
        .unwrap();
        let config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(Some(proxy.endpoint().to_owned()))
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            )
            .with_allow_http(true);
        Some((
            stack,
            proxy,
            database,
            ServerObjectStore::s3(config).unwrap(),
        ))
    } else {
        None
    };
    #[cfg(feature = "docker")]
    let objects = remote_fixture
        .as_ref()
        .map_or(objects, |fixture| fixture.3.clone());
    let mut backend = LocalBackend::new_with_object_store(
        root.path().to_owned(),
        base.clone(),
        chunk_size,
        objects.clone(),
    )
    .await
    .unwrap();
    let work = BoundedPool::new(NonZeroUsize::new(1).unwrap());
    backend.stream_work_pool = work.clone();
    let backend = ServerBackend::Local(backend);
    #[cfg(feature = "docker")]
    let backend = if let Some(fixture) = &remote_fixture {
        let mut backend = crate::PostgresBackend::new_with_object_store(
            root.path().to_owned(),
            base.clone(),
            chunk_size,
            &fixture.2,
            objects.clone(),
        )
        .await
        .unwrap();
        backend.stream_work_pool = work.clone();
        ServerBackend::Postgres(backend)
    } else {
        backend
    };
    let key = b"http-download-disconnect-signing-key";
    let state = Arc::new(AppState {
        config: ServerConfig::new(address, base.clone(), root.path().to_owned(), chunk_size),
        role: ServerRole::All,
        backend,
        auth: Some(ServerAuth::new(key).unwrap()),
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        admission: WeightedAdmission::new(NonZeroUsize::new(256).unwrap()),
        pools: ExecutionPools::default_sizes(),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        protocol_metrics: ProtocolMetrics::default(),
    });
    let router = Router::new()
        .route(
            "/v1/xorbs/default/{hash}",
            post(super::super::operational::upload_xorb),
        )
        .route("/v1/shards", post(super::super::operational::upload_shard))
        .route(
            "/v1/hub/xet-files/{sha256}",
            get(super::download_hub_xet_file),
        )
        .with_state(state.clone());
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let token = TokenSigner::new(key)
        .unwrap()
        .sign(
            &TokenClaims::new(
                "fixture",
                "operator",
                TokenScope::Write,
                RepositoryScope::new(
                    RepositoryProvider::Generic,
                    "team",
                    "disconnect",
                    Some("main"),
                )
                .unwrap(),
                u64::MAX,
            )
            .unwrap(),
        )
        .unwrap();
    let first = vec![1_u8; 65536];
    let second = vec![2_u8; 65536];
    let (first_xorb, first_hash) = crate::test_fixtures::single_chunk_xorb(&first);
    let (second_xorb, second_hash) = crate::test_fixtures::single_chunk_xorb(&second);
    let client = reqwest::Client::new();
    for (bytes, hash) in [(first_xorb, &first_hash), (second_xorb, &second_hash)] {
        client
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let (shard, _) =
        crate::test_fixtures::single_file_shard(&[(&first, &first_hash), (&second, &second_hash)]);
    client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(&token)
        .body(shard)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let content = [first, second].concat();
    let digest = hex::encode(Sha256::digest(&content));
    let (started, running) = tokio::sync::oneshot::channel();
    let (resume, wait) = std::sync::mpsc::channel();
    if !remote {
        let path = objects
            .local_path_for_key(&crate::xet_adapter::xorb_object_key(&second_hash).unwrap())
            .unwrap();
        set_before_local_object_read_hook(path, move || {
            let _sent = started.send(());
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
        });
    } else {
        #[cfg(feature = "docker")]
        {
            let fixture = remote_fixture.as_ref().unwrap();
            fixture.1.arm();
            // Remote readiness is checked below using the injected provider GET;
            // this channel is only the local hook's rendezvous.
            let _sent = started.send(());
        }
    }
    #[cfg(feature = "docker")]
    let reads_before = remote_fixture
        .as_ref()
        .map(|fixture| fixture.1.get_requests());
    let baseline = state.admission.available_permits();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!(
        "GET /v1/hub/xet-files/{digest} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    ).as_bytes()).await.unwrap();
    #[cfg(feature = "docker")]
    if let Some(fixture) = &remote_fixture {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !fixture.1.fault_was_injected() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("remote source GET must reach the injected delay");
    }
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .unwrap()
        .unwrap();
    assert!(state.admission.available_permits() < baseline);
    assert!(work.try_acquire().is_none());
    drop(socket);
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.admission.available_permits() != baseline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("HTTP disconnect must drop the response and request admission");
    assert!(
        work.try_acquire().is_none(),
        "running work remains charged until it stops"
    );
    if !remote {
        resume.send(()).unwrap();
    }
    let permit = tokio::time::timeout(Duration::from_secs(20), work.acquire())
        .await
        .unwrap()
        .unwrap();
    drop(permit);
    #[cfg(feature = "docker")]
    if let Some(fixture) = &remote_fixture {
        assert_eq!(
            fixture.1.get_requests().checked_sub(reads_before.unwrap()),
            Some(1),
            "cancelled download must not fetch the next Xorb"
        );
    }
    let downloaded = client
        .get(format!("{base}/v1/hub/xet-files/{digest}"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(downloaded.as_ref(), content);
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.admission.available_permits() != baseline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("verified retry must release response admission");
}
