use std::{
    error::Error as StdError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use axum::body::Bytes;
use shardline_index::{IndexStore, LocalIndexStore};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use tokio::{net::TcpListener, spawn, task::JoinHandle};

use super::super::{serve_with_listener, wait_for_health, write_provider_config};
use crate::{
    LocalBackend, ServerConfig, ServerError, test_invariant_error::ServerTestInvariantError,
};

pub(super) const PROVIDER_BOOTSTRAP_KEY: &str = "provider-bootstrap";
pub(super) const TOKEN_SIGNING_KEY: &[u8] = b"signing-key";

pub(super) struct ProviderHttpRuntime {
    storage: tempfile::TempDir,
    base_url: String,
    server: JoinHandle<Result<(), ServerError>>,
}

impl ProviderHttpRuntime {
    pub(super) fn storage_path(&self) -> &Path {
        self.storage.path()
    }

    pub(super) fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for ProviderHttpRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub(super) async fn start_provider_runtime(
    write_config: fn(&Path) -> Result<PathBuf, Box<dyn StdError>>,
    issuer: &str,
    token_ttl: NonZeroU64,
) -> Result<ProviderHttpRuntime, Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let provider_config = write_config(storage.path())?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(TOKEN_SIGNING_KEY.to_vec())
    .and_then(|config| {
        config.with_provider_runtime(
            provider_config,
            PROVIDER_BOOTSTRAP_KEY.as_bytes().to_vec(),
            issuer.to_owned(),
            token_ttl,
        )
    })?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    if let Err(error) = wait_for_health(&base_url).await {
        server.abort();
        return Err(error);
    }

    Ok(ProviderHttpRuntime {
        storage,
        base_url,
        server,
    })
}

pub(super) async fn start_github_provider_runtime(
    token_ttl: NonZeroU64,
) -> Result<ProviderHttpRuntime, Box<dyn StdError>> {
    start_provider_runtime(write_provider_config, "github-app", token_ttl).await
}

pub(super) async fn upload_repository_asset(
    runtime: &ProviderHttpRuntime,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) -> Result<(), Box<dyn StdError>> {
    let backend = LocalBackend::new(
        runtime.storage_path().to_path_buf(),
        runtime.base_url().to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let scope = RepositoryScope::new(provider, owner, repo, Some("main"))?;
    let _upload = backend
        .upload_file("asset.bin", Bytes::from_static(b"abcdefgh"), Some(&scope))
        .await?;
    Ok(())
}

pub(super) fn retention_hold_count(root: &Path) -> Result<usize, Box<dyn StdError>> {
    let index_store = LocalIndexStore::new(root.to_path_buf())?;
    let holds = IndexStore::list_retention_holds(&index_store)?;
    Ok(holds.len())
}

pub(super) fn invariant(message: &'static str) -> Box<dyn StdError> {
    ServerTestInvariantError::new(message).into()
}
