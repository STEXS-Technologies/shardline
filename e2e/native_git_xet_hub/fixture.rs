//! Independently rooted Hub nodes sharing only the configured durable backends.
use super::*;
use std::sync::atomic::AtomicUsize;

pub(super) const SIGNING_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

#[derive(Clone, Default)]
pub(super) struct Counts {
    pub(super) xorbs: Arc<AtomicUsize>,
    pub(super) shards: Arc<AtomicUsize>,
    pub(super) basic: Arc<AtomicUsize>,
    pub(super) downloads: Arc<AtomicUsize>,
}

struct Server(tokio::task::JoinHandle<Result<(), std::io::Error>>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) struct HubNode {
    pub(super) base: String,
    server: Server,
    background: Option<shardline_server::app::RouterBackgroundTasks>,
    _storage: tempfile::TempDir,
}

impl HubNode {
    pub(super) async fn start(
        services: Option<&shardline_test_support::DockerLocalStack>,
        prefix: &str,
        counts: Counts,
    ) -> Result<Self, TestError> {
        let storage = tempfile::tempdir()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let base = format!("http://{address}");
        let mut config = ServerConfig::new(
            address,
            base.clone(),
            storage.path().to_owned(),
            NonZeroUsize::new(65536).ok_or("chunk size")?,
        )
        .with_token_signing_key(SIGNING_KEY.to_vec())?
        .with_server_frontends([shardline_server::ServerFrontend::Hub])?
        .with_hub_webhook_secret_key(vec![7; 32])?
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure);
        if let Some(stack) = services {
            let url = stack
                .postgres_url()
                .ok_or("remote git-xet PostgreSQL missing")?;
            let pool = sqlx::PgPool::connect(&url).await?;
            shardline_server::apply_database_migrations(&pool).await?;
            pool.close().await;
            let raw = stack
                .s3_raw_config(Some(prefix))
                .ok_or("remote git-xet MinIO missing")?;
            let objects = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
                .with_endpoint(raw.endpoint)
                .with_credentials(
                    raw.access_key.map(shardline_protocol::SecretString::new),
                    raw.secret_key.map(shardline_protocol::SecretString::new),
                    raw.session_token.map(shardline_protocol::SecretString::new),
                )
                .with_key_prefix(raw.key_prefix.as_deref())
                .with_allow_http(raw.allow_http);
            config = config
                .with_index_postgres_url(url)?
                .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(objects));
        }
        let (router, background) =
            shardline_server::app::router_with_background_tasks(config).await?;
        let router = router.layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let counts = counts.clone();
                async move {
                    let path = request.uri().path();
                    if request.method() == Method::POST && path.starts_with("/v1/xorbs/") {
                        counts.xorbs.fetch_add(1, Ordering::Relaxed);
                    }
                    if request.method() == Method::POST
                        && matches!(path, "/shards" | "/v1/shards" | "/v2/shards")
                    {
                        counts.shards.fetch_add(1, Ordering::Relaxed);
                    }
                    if request.method() == Method::PUT && path.contains("/lfs/") {
                        counts.basic.fetch_add(1, Ordering::Relaxed);
                    }
                    if request.method() == Method::GET && path.starts_with("/lfs/objects/") {
                        counts.downloads.fetch_add(1, Ordering::Relaxed);
                    }
                    next.run(request).await
                }
            },
        ));
        Ok(Self {
            base,
            server: Server(tokio::spawn(
                async move { axum::serve(listener, router).await },
            )),
            background: Some(background),
            _storage: storage,
        })
    }

    pub(super) async fn stop(&mut self) -> Result<(), TestError> {
        self.server.0.abort();
        match (&mut self.server.0).await {
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(error.into()),
            Ok(result) => result?,
        }
        if let Some(background) = self.background.take() {
            background.shutdown().await?;
        }
        Ok(())
    }
}
