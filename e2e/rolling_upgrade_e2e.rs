//! Rolling-upgrade e2e: proves the API and transfer roles can be upgraded and
//! rolled back independently of each other.
//!
//! Two role-split servers run side by side (one `api`, one `transfer`) on
//! distinct ephemeral ports. One role is stopped (mimicking a rolling
//! upgrade); the other must keep serving its role with zero interruption, and
//! then the upgraded server is brought back on the SAME port and must be
//! healthy again.
//!
//! Role-surface probing mirrors `role_split_e2e.rs`: the reconstruction route
//! (`/v1/reconstructions/...`) is served by the api role and the chunk route
//! (`/v1/chunks/...`) by the transfer role, and both routes are GET-only, so
//! an unmounted route returns 404 while a mounted route rejects the wrong
//! method with 405.

mod support;

use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::Path,
    time::Duration,
};

use axum::http::{Method, StatusCode};
use reqwest::Client;
use shardline_server::{
    ReadyResponse, ServerConfig, ServerError, ServerFrontend, ServerRole, serve_with_listener,
};
use support::ServerE2eInvariantError;
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::sleep,
};

const SIGNING_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";
const FRONTENDS: [ServerFrontend; 1] = [ServerFrontend::Xet];

/// A running role-split server runtime.
struct RoleRuntime {
    addr: SocketAddr,
    base_url: String,
    server: Option<JoinHandle<Result<(), ServerError>>>,
}

impl RoleRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Stops the server and waits for its task to fully unwind. Mimics a
    /// graceful rolling-upgrade stop: the listener is dropped before this
    /// returns, so the same port can be rebound immediately.
    async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(server) = self.server.take() else {
            return Ok(());
        };
        server.abort();
        match server.await {
            Ok(result) => result.map_err(|error| {
                format!("server task failed during upgrade stop: {error}").into()
            }),
            Err(join_error) if join_error.is_cancelled() => Ok(()),
            Err(join_error) => Err(format!("server task join failed: {join_error}").into()),
        }
    }
}

impl Drop for RoleRuntime {
    fn drop(&mut self) {
        if let Some(server) = &self.server {
            server.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_role_rolling_upgrade_keeps_transfer_serving() {
    let result = exercise_rolling_upgrade(ServerRole::Api, ServerRole::Transfer).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "api rolling-upgrade e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_role_rolling_upgrade_keeps_api_serving() {
    let result = exercise_rolling_upgrade(ServerRole::Transfer, ServerRole::Api).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "transfer rolling-upgrade e2e failed: {error:?}"
    );
}

/// Brings up `upgrade_role` and `steady_role` on distinct ephemeral ports,
/// waits for both to be ready, stops the upgraded role, asserts the steady
/// role kept serving its full role surface, then brings the upgraded role back
/// on its ORIGINAL port and asserts it is healthy again.
async fn exercise_rolling_upgrade(
    upgrade_role: ServerRole,
    steady_role: ServerRole,
) -> Result<(), Box<dyn Error>> {
    let upgrade_storage = tempfile::tempdir()?;
    let steady_storage = tempfile::tempdir()?;

    let mut upgrade = start_role_runtime(upgrade_role, &FRONTENDS, None, upgrade_storage.path()).await?;
    let mut steady = start_role_runtime(steady_role, &FRONTENDS, None, steady_storage.path()).await?;

    let client = Client::new();
    wait_for_ready(&client, upgrade.base_url()).await?;
    wait_for_ready(&client, steady.base_url()).await?;

    let upgrade_ready = ready_response(&client, upgrade.base_url()).await?;
    let steady_ready = ready_response(&client, steady.base_url()).await?;
    assert_eq!(upgrade_ready.server_role, upgrade_role.as_str());
    assert_eq!(steady_ready.server_role, steady_role.as_str());
    assert_eq!(upgrade_ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(steady_ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(
        role_surface_statuses(&client, upgrade.base_url()).await?,
        expected_surface(upgrade_role)
    );
    assert_eq!(
        role_surface_statuses(&client, steady.base_url()).await?,
        expected_surface(steady_role)
    );

    let upgraded_addr = upgrade.addr;
    let upgraded_base_url = upgrade.base_url().to_owned();

    // Upgrade window: stop the upgraded server...
    upgrade.stop().await?;
    wait_for_server_down(&client, &upgraded_base_url).await?;

    // ...and prove the steady server never blinked: readiness plus its full
    // role surface (api-only reconstruction route and transfer-only chunk
    // route) still answer exactly as before.
    let steady_ready = ready_response(&client, steady.base_url()).await?;
    assert_eq!(steady_ready.server_role, steady_role.as_str());
    assert_eq!(
        role_surface_statuses(&client, steady.base_url()).await?,
        expected_surface(steady_role)
    );

    // Roll the upgraded server back on the SAME port it served before.
    let mut upgraded_restarted = start_role_runtime(
        upgrade_role,
        &FRONTENDS,
        Some(upgraded_addr),
        upgrade_storage.path(),
    )
    .await?;
    wait_for_ready(&client, upgraded_restarted.base_url()).await?;
    let restarted_ready = ready_response(&client, upgraded_restarted.base_url()).await?;
    assert_eq!(restarted_ready.server_role, upgrade_role.as_str());
    assert_eq!(restarted_ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(
        role_surface_statuses(&client, upgraded_restarted.base_url()).await?,
        expected_surface(upgrade_role)
    );

    // The steady server is still serving alongside the rolled-back one.
    let steady_ready = ready_response(&client, steady.base_url()).await?;
    assert_eq!(steady_ready.server_role, steady_role.as_str());

    upgraded_restarted.stop().await?;
    steady.stop().await?;
    Ok(())
}

/// Starts a role-split server. `bind_addr: None` binds an ephemeral port;
/// `Some(addr)` (re)binds a specific port, with a small backoff retry loop in
/// case the OS has not yet released it after the previous listener dropped.
async fn start_role_runtime(
    role: ServerRole,
    frontends: &[ServerFrontend],
    bind_addr: Option<SocketAddr>,
    storage: &Path,
) -> Result<RoleRuntime, Box<dyn Error>> {
    let listener = bind_with_retry(bind_addr.unwrap_or_else(ephemeral_addr), 20).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.to_path_buf(),
        NonZeroUsize::new(128).ok_or("chunk size")?,
    )
    .with_server_role(role)
    .with_token_signing_key(SIGNING_KEY.to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    let client = Client::new();
    wait_for_health(&client, &base_url).await?;
    Ok(RoleRuntime {
        addr,
        base_url,
        server: Some(server),
    })
}

fn ephemeral_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn bind_with_retry(addr: SocketAddr, attempts: u32) -> Result<TcpListener, Box<dyn Error>> {
    for attempt in 0..attempts {
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(_error) if attempt + 1 < attempts => {
                sleep(Duration::from_millis(50 * u64::from(attempt + 1))).await;
            }
            Err(error) => {
                return Err(format!("failed to bind {addr} after {attempts} attempts: {error}").into());
            }
        }
    }
    unreachable!("bind_with_retry loop must return")
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

async fn wait_for_ready(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
    for _attempt in 0..100 {
        if let Ok(response) = client.get(format!("{base_url}/readyz")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(ServerE2eInvariantError::new("server did not become ready").into())
}

/// Asserts the stopped server can no longer be reached, proving the upgrade
/// window is real (the old listener is gone before the new one rebinds).
async fn wait_for_server_down(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
    for _attempt in 0..50 {
        if client.get(format!("{base_url}/healthz")).send().await.is_err() {
            return Ok(());
        }
        // Raw connect probe (bypasses any reqwest connection pooling): if the
        // listener is gone this must be refused.
        let raw = tokio::net::TcpStream::connect(
            base_url
                .trim_start_matches("http://")
                .parse::<SocketAddr>()
                .map_err(|error| format!("bad base url {base_url}: {error}"))?,
        )
        .await;
        if raw.is_err() {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(ServerE2eInvariantError::new("server did not go down after upgrade stop").into())
}

async fn ready_response(
    client: &Client,
    base_url: &str,
) -> Result<ReadyResponse, Box<dyn Error>> {
    let response = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<ReadyResponse>().await?)
}

/// Probes the two role-specific surfaces, mirroring `role_split_e2e.rs`:
/// returns (reconstruction-route status, chunk-route status).
async fn role_surface_statuses(
    client: &Client,
    base_url: &str,
) -> Result<(StatusCode, StatusCode), Box<dyn Error>> {
    let reconstruction_status = client
        .request(Method::PUT, format!("{base_url}/v1/reconstructions/asset.bin"))
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
    Ok((reconstruction_status, chunk_status))
}

/// Expected (reconstruction, chunk) statuses for a role. Both routes are
/// GET-only, so a mounted route rejects the probe method with 405 while an
/// unmounted route returns 404.
fn expected_surface(role: ServerRole) -> (StatusCode, StatusCode) {
    match role {
        ServerRole::Api => (StatusCode::METHOD_NOT_ALLOWED, StatusCode::NOT_FOUND),
        ServerRole::Transfer => (StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED),
        ServerRole::All => unreachable!("rolling-upgrade e2e only exercises split roles"),
    }
}
