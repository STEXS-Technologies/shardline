//! Real PostgreSQL streaming-replica promotion drill.
//!
//! The test keeps one Shardline process alive while its SQLx connections lose
//! the primary. A stable TCP endpoint is switched to the promoted standby;
//! acknowledged pre-failover bytes and fresh post-failover publication must
//! both remain exact.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::panic,
    clippy::unwrap_used
)]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    process::Command,
    sync::Arc,
    time::Duration,
};

use reqwest::{Client, StatusCode};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, unix_now_seconds_lossy,
};
use shardline_server::{
    ServerConfig, ServerFrontend, apply_database_migrations, serve_with_listener,
};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tempfile::TempDir;
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::RwLock,
    task::JoinHandle,
    time::sleep,
};

const PRIMARY_CONTAINER: &str = "shardline-failover-primary";
const STANDBY_CONTAINER: &str = "shardline-failover-standby";
const PRIMARY_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 25_432);
const STANDBY_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 25_433);
const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const BUCKET: &str = "failover.failover";

struct SwitchingTcpProxy {
    addr: SocketAddr,
    upstream: Arc<RwLock<SocketAddr>>,
    task: JoinHandle<()>,
}

impl SwitchingTcpProxy {
    async fn start(initial_upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Arc::new(RwLock::new(initial_upstream));
        let task_upstream = upstream.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut downstream, _peer)) = listener.accept().await {
                let connection_upstream = task_upstream.clone();
                tokio::spawn(async move {
                    let target = *connection_upstream.read().await;
                    if let Ok(mut upstream_stream) = TcpStream::connect(target).await {
                        let _ = copy_bidirectional(&mut downstream, &mut upstream_stream).await;
                    }
                });
            }
        });
        Self {
            addr,
            upstream,
            task,
        }
    }

    async fn switch_to(&self, upstream: SocketAddr) {
        *self.upstream.write().await = upstream;
    }

    fn database_url(&self) -> String {
        format!(
            "postgres://shardline:shardline-dev-password@{}/shardline",
            self.addr
        )
    }
}

impl Drop for SwitchingTcpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn docker(args: &[&str]) -> std::process::Output {
    Command::new("docker").args(args).output().unwrap()
}

fn failover_stack_available() -> bool {
    docker(&["inspect", PRIMARY_CONTAINER, STANDBY_CONTAINER])
        .status
        .success()
}

fn mint_token() -> String {
    let provider = LocalHmacProvider::new(SIGNING_KEY).unwrap();
    let repository = RepositoryScope::new(
        RepositoryProvider::Generic,
        "failover",
        "failover",
        Some("main"),
    )
    .unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "postgres-failover",
        TokenScope::Write,
        repository,
        unix_now_seconds_lossy().saturating_add(600),
    )
    .unwrap();
    provider.mint_token(&claims).unwrap()
}

async fn wait_http_ready(client: &Client, base_url: &str) {
    for _attempt in 0..100 {
        if matches!(
            client
                .get(format!("{base_url}/readyz"))
                .timeout(Duration::from_secs(1))
                .send()
                .await,
            Ok(response) if response.status().is_success()
        ) {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Shardline did not become ready");
}

async fn wait_replica_replays(pool: &PgPool, target_lsn: &str) {
    for _attempt in 0..100 {
        let replayed =
            sqlx::query_scalar::<_, bool>("SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn")
                .bind(target_lsn)
                .fetch_one(pool)
                .await
                .unwrap_or(false);
        if replayed {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("standby did not replay primary WAL position {target_lsn}");
}

async fn wait_for_exact_get(client: &Client, base_url: &str, token: &str, key: &str, bytes: &[u8]) {
    for _attempt in 0..120 {
        if let Ok(response) = client
            .get(format!("{base_url}/{BUCKET}/{key}"))
            .bearer_auth(token)
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && response.status() == StatusCode::OK
            && let Ok(observed) = response.bytes().await
            && observed.as_ref() == bytes
        {
            return;
        }
        sleep(Duration::from_millis(250)).await;
    }
    panic!("Shardline did not recover exact bytes for {key} after promotion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_server_recovers_after_real_primary_promotion() {
    if !failover_stack_available() {
        eprintln!(
            "SKIPPED: PostgreSQL failover stack unavailable; run `docker compose -f docker-compose.postgres-failover.yml up -d --wait`"
        );
        return;
    }

    let proxy = SwitchingTcpProxy::start(PRIMARY_ADDR).await;
    let database_url = proxy.database_url();
    let migration_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    apply_database_migrations(&migration_pool).await.unwrap();

    let root = TempDir::new().unwrap();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        root.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::S3])
    .unwrap()
    .with_token_signing_key(SIGNING_KEY.to_vec())
    .unwrap()
    .with_index_postgres_url(database_url)
    .unwrap();
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    let client = Client::new();
    let base_url = format!("http://{addr}");
    wait_http_ready(&client, &base_url).await;

    let token = mint_token();
    let before = b"acknowledged-before-real-primary-promotion";
    let pre_failover_response = client
        .put(format!("{base_url}/{BUCKET}/before"))
        .bearer_auth(&token)
        .body(before.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(pre_failover_response.status(), StatusCode::OK);

    let primary_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://shardline:shardline-dev-password@127.0.0.1:25432/shardline")
        .await
        .unwrap();
    let primary_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&primary_pool)
        .await
        .unwrap();
    let standby_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://shardline:shardline-dev-password@127.0.0.1:25433/shardline")
        .await
        .unwrap();
    wait_replica_replays(&standby_pool, &primary_lsn).await;
    primary_pool.close().await;
    standby_pool.close().await;
    migration_pool.close().await;

    let killed = docker(&["kill", "--signal=KILL", PRIMARY_CONTAINER]);
    assert!(killed.status.success(), "kill primary: {:?}", killed.stderr);
    let promoted = docker(&[
        "exec",
        "--user",
        "postgres",
        STANDBY_CONTAINER,
        "pg_ctl",
        "-D",
        "/var/lib/postgresql/data",
        "promote",
        "-w",
    ]);
    assert!(
        promoted.status.success(),
        "promote standby: {}",
        String::from_utf8_lossy(&promoted.stderr)
    );
    proxy.switch_to(STANDBY_ADDR).await;

    wait_for_exact_get(&client, &base_url, &token, "before", before).await;
    let after = b"published-after-real-standby-promotion";
    let post_promotion_response = client
        .put(format!("{base_url}/{BUCKET}/after"))
        .bearer_auth(&token)
        .body(after.as_slice())
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(post_promotion_response.status(), StatusCode::OK);
    wait_for_exact_get(&client, &base_url, &token, "after", after).await;
    assert!(
        !server.is_finished(),
        "server exited during database failover"
    );
    server.abort();
}
