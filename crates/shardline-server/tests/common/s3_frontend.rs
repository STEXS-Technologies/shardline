//! Shared hermetic HTTP fixture for S3 frontend and native-client tests.

use std::num::NonZeroUsize;

use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use tempfile::TempDir;
use tokio::net::TcpListener;

pub(super) const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

pub(super) struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    pub(super) base_url: String,
    _tmp: TempDir,
}

impl TestServer {
    /// Starts a full shardline server with the S3 frontend, backed by SQLite
    /// metadata + a local object store (hermetic — no Docker).
    pub(super) async fn start() -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(vec![ServerFrontend::S3])
        .unwrap()
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled();

        config.validate_runtime_requirements().unwrap();
        let app = app::router(config).await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            _tmp: tmp,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}
