pub use shardline_test_support::InvariantError as ServerE2eInvariantError;

use std::{
    error::Error as StdError,
    fs::write as write_file,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::body::Bytes;
use futures_util::stream;
use hmac::Mac;
use reqwest::Client;
use serde_json::{json, to_vec};
use shardline_index::{LifecycleStore, LocalIndexStore, ProviderRepositoryState};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_server::{
    ServerByteStream, ServerConfig, ServerError, ServerFrontend,
    test_invariant_error::ServerTestInvariantError,
};
use tokio::{net::TcpListener, sync::OwnedSemaphorePermit, time::sleep};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;

pub(crate) fn bearer_token(
    subject: &str,
    scope: TokenScope,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
    revision: Option<&str>,
) -> Result<String, Box<dyn StdError>> {
    let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!")?;
    let repository = RepositoryScope::new(provider, owner, repo, revision)?;
    let claims = TokenClaims::new("local", subject, scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

pub(crate) async fn wait_for_health(base_url: &str) -> Result<(), Box<dyn StdError>> {
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

    Err(ServerTestInvariantError::new("server did not become healthy").into())
}

pub(crate) fn test_byte_stream(items: Vec<Result<Bytes, ServerError>>) -> ServerByteStream {
    Box::pin(stream::iter(items))
}

pub(crate) fn write_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "github",
                "integration_subject": "github-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://github.example/team/assets.git",
                        "read_subjects": ["github-user-1"],
                        "write_subjects": ["github-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

pub(crate) fn write_gitlab_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-gitlab.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "gitlab",
                "integration_subject": "gitlab-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "group",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://gitlab.example/group/assets.git",
                        "read_subjects": ["gitlab-user-1"],
                        "write_subjects": ["gitlab-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

pub(crate) fn write_gitea_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-gitea.json");
    let bytes = to_vec(&json!({
        "providers": [
            {
                "kind": "gitea",
                "integration_subject": "gitea-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "team",
                        "name": "assets",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://gitea.example/team/assets.git",
                        "read_subjects": ["gitea-user-1"],
                        "write_subjects": ["gitea-user-1"]
                    }
                ]
            }
        ]
    }))?;
    write_file(&path, bytes)?;
    Ok(path)
}

pub(crate) fn write_generic_provider_config(root: &Path) -> Result<PathBuf, Box<dyn StdError>> {
    let path = root.join("providers-generic.json");
    let bytes = to_vec(&json!({
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

pub(crate) fn seed_provider_repository_state(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) {
    let index_store = LocalIndexStore::new(root.to_path_buf());
    assert!(index_store.is_ok());
    let Ok(index_store) = index_store else {
        return;
    };
    let state = ProviderRepositoryState::new(
        provider,
        owner.to_owned(),
        repo.to_owned(),
        Some(7),
        Some(11),
        Some("refs/heads/seeded".to_owned()),
    );
    let persisted = LifecycleStore::upsert_provider_repository_state(&index_store, &state);
    assert!(persisted.is_ok());
}

pub(crate) fn assert_provider_repository_state_absent(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) {
    let state = provider_repository_state(root, provider, owner, repo);
    assert!(state.is_none());
}

pub(crate) fn assert_provider_repository_state_migrated(
    root: &Path,
    provider: RepositoryProvider,
    old_owner: &str,
    old_repo: &str,
    new_owner: &str,
    new_repo: &str,
) {
    assert_provider_repository_state_absent(root, provider, old_owner, old_repo);
    let state = provider_repository_state(root, provider, new_owner, new_repo);
    assert!(state.is_some());
    let Some(state) = state else {
        return;
    };
    assert_eq!(state.provider(), provider);
    assert_eq!(state.owner(), new_owner);
    assert_eq!(state.repo(), new_repo);
    assert_eq!(state.last_access_changed_at_unix_seconds(), Some(7));
    assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(11));
    assert_eq!(state.last_pushed_revision(), Some("refs/heads/seeded"));
}

pub(crate) fn assert_provider_repository_state_observed(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
    access_changed: bool,
    revision: Option<&str>,
) {
    let state = provider_repository_state(root, provider, owner, repo);
    assert!(state.is_some());
    let Some(state) = state else {
        return;
    };
    assert_eq!(state.provider(), provider);
    assert_eq!(state.owner(), owner);
    assert_eq!(state.repo(), repo);
    assert_eq!(
        state.last_access_changed_at_unix_seconds().is_some(),
        access_changed
    );
    assert_eq!(
        state.last_revision_pushed_at_unix_seconds().is_some(),
        revision.is_some()
    );
    assert_eq!(state.last_pushed_revision(), revision);
}

pub(crate) fn provider_repository_state(
    root: &Path,
    provider: RepositoryProvider,
    owner: &str,
    repo: &str,
) -> Option<ProviderRepositoryState> {
    let index_store = LocalIndexStore::new(root.to_path_buf());
    assert!(index_store.is_ok());
    let Ok(index_store) = index_store else {
        return None;
    };
    let state = LifecycleStore::provider_repository_state(&index_store, provider, owner, repo);
    assert!(state.is_ok());
    let Ok(state) = state else {
        return None;
    };
    state
}

pub(crate) fn github_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

pub(crate) fn gitea_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(hex::encode(mac.finalize().into_bytes()))
}

pub(crate) fn generic_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

pub(crate) async fn start_server(
    frontends: &[ServerFrontend],
) -> Result<(String, tokio::task::JoinHandle<Result<(), ServerError>>), Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server =
        tokio::spawn(async move { shardline_server::serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;
    Ok((base_url, server))
}

pub(crate) async fn start_server_with_config(
    config: ServerConfig,
) -> Result<(String, tokio::task::JoinHandle<Result<(), ServerError>>), Box<dyn StdError>> {
    let listener = TcpListener::bind(config.bind_addr()).await?;
    let base_url = config.public_base_url().to_owned();
    let server =
        tokio::spawn(async move { shardline_server::serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;
    Ok((base_url, server))
}
