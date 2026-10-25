//! Progress and disconnects during a stalled read, with real HTTP and backends.

use crate::{ServerConfig, serve_with_listener};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    response::Response,
    routing::any,
};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Proxy {
    origin: String,
    source: String,
    reached: Notify,
    release: Notify,
    client: reqwest::Client,
    hold: AtomicBool,
}

struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn forward(State(state): State<Arc<Proxy>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    if parts.method == axum::http::Method::GET
        && parts.uri.path().contains(&state.source)
        && state.hold.swap(false, Ordering::SeqCst)
    {
        state.reached.notify_one();
        state.release.notified().await;
    }
    // Preserve the original Host covered by the client's S3 signature.
    let headers = parts.headers;
    let result = async {
        let bytes = to_bytes(body, 1024 * 1024).await?;
        let response = state
            .client
            .request(parts.method, format!("{}{}", state.origin, parts.uri))
            .headers(headers)
            .body(bytes)
            .send()
            .await?;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.bytes().await?;
        let mut output = Response::new(Body::from(bytes));
        *output.status_mut() = status;
        *output.headers_mut() = headers;
        Ok::<_, Error>(output)
    }
    .await;
    result.unwrap_or_else(|error| {
        drop(error);
        Response::builder()
            .status(502)
            .body(Body::from("fixture proxy failed"))
            .expect("response")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and MinIO"]
async fn disconnect_during_remote_shard_verification_prevents_publication_and_allows_retry()
-> Result<(), Error> {
    stalled_verification(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and MinIO"]
async fn stalled_remote_shard_verification_keeps_progress_live_and_publishes_after_release()
-> Result<(), Error> {
    stalled_verification(false).await
}

async fn stalled_verification(disconnect: bool) -> Result<(), Error> {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
    })
    .await??
    .ok_or("Docker is required")?;
    let database = stack.postgres_url().ok_or("PostgreSQL is required")?;
    let owner = sqlx::PgPool::connect(&database).await?;
    crate::apply_database_migrations(&owner).await?;
    let raw = stack
        .s3_raw_config(Some("shard-disconnect"))
        .ok_or("MinIO is required")?;
    let content = b"a verified member must not publish after its HTTP owner disconnects";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(content);
    let (shard, file) = crate::test_fixtures::single_file_shard(&[(content, &hash)]);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_url = format!("http://{}", listener.local_addr()?);
    let proxy = Arc::new(Proxy {
        origin: raw.endpoint.ok_or("MinIO endpoint")?,
        source: hash.clone(),
        reached: Notify::new(),
        release: Notify::new(),
        client: reqwest::Client::new(),
        hold: AtomicBool::new(false),
    });
    let router = Router::new()
        .fallback(any(forward))
        .with_state(proxy.clone());
    let _proxy_task = Task(tokio::spawn(async move {
        axum::serve(listener, router).await.expect("proxy");
    }));
    let objects = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy_url))
        .with_allow_http(true)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let root = tempfile::tempdir()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let base = format!("http://{address}");
    let key = b"shard-disconnect-test-signing-key-32";
    let config = ServerConfig::new(
        address,
        base.clone(),
        root.path().to_owned(),
        NonZeroUsize::new(65536).ok_or("chunk size")?,
    )
    .with_token_signing_key(key.to_vec())?
    .with_admission_max_weight(
        NonZeroUsize::new(usize::try_from(crate::admission::weights::SHARD_UPLOAD)?)
            .ok_or("shard admission weight")?,
    )
    .with_index_postgres_url(database.clone())?
    .with_object_storage(crate::ObjectStorageAdapter::S3, Some(objects));
    let _server = Task(tokio::spawn(async move {
        serve_with_listener(config, listener).await.expect("server");
    }));
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .get(format!("{base}/healthz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|error| std::io::Error::other(format!("server health readiness: {error}")))?;
    let scope = RepositoryScope::new(
        RepositoryProvider::Generic,
        "team",
        "disconnect",
        Some("main"),
    )?;
    let token = TokenSigner::new(key)?.sign(&TokenClaims::new(
        "fixture",
        "operator",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    client
        .post(format!("{base}/v1/xorbs/default/{hash}"))
        .bearer_auth(&token)
        .body(xorb)
        .send()
        .await?
        .error_for_status()?;
    let mut socket = TcpStream::connect(address).await?;
    proxy.hold.store(true, Ordering::SeqCst);
    let request = format!(
        "POST /v2/shards HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        shard.len()
    );
    socket.write_all(request.as_bytes()).await?;
    socket.write_all(&shard).await?;
    let mut first = [0_u8; 4096];
    let received = tokio::time::timeout(Duration::from_secs(10), socket.read(&mut first))
        .await
        .map_err(|error| std::io::Error::other(format!("streaming response start: {error}")))??;
    if received == 0 {
        return Err("streaming response must begin".into());
    }
    tokio::time::timeout(Duration::from_secs(10), proxy.reached.notified())
        .await
        .map_err(|error| std::io::Error::other(format!("verification source read: {error}")))?;
    let mut response = first[..received].to_vec();
    tokio::time::timeout(Duration::from_secs(10), async {
        while String::from_utf8_lossy(&response)
            .matches("\"type\":\"validating\"")
            .count()
            < 2
        {
            let received = socket.read(&mut first).await?;
            if received == 0 {
                return Err(std::io::Error::other(
                    "progress ended during stalled storage",
                ));
            }
            response.extend_from_slice(&first[..received]);
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|error| std::io::Error::other(format!("stalled verification heartbeat: {error}")))??;
    if String::from_utf8_lossy(&response).contains("\"type\":\"result\"") {
        return Err("stalled source read reported premature success".into());
    }
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id=$1")
            .bind(&file)
            .fetch_one(&owner)
            .await?;
    if pending != 0 {
        return Err("stalled verification published before source validation".into());
    }
    // A live streaming request must retain admission while it waits for storage.
    // Reject excess work immediately instead of starting another verification.
    let saturated = client
        .post(format!("{base}/v2/shards"))
        .bearer_auth(&token)
        .body(shard.clone())
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if saturated.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE
        || saturated.headers().get(reqwest::header::RETRY_AFTER)
            != Some(&reqwest::header::HeaderValue::from_static("1"))
    {
        return Err(
            "stalled verification must retain admission and return retryable saturation".into(),
        );
    }
    drop(saturated);
    if !disconnect {
        proxy.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut response))
            .await
            .map_err(|error| {
                std::io::Error::other(format!("released verification response: {error}"))
            })??;
        if !String::from_utf8_lossy(&response).contains("\"type\":\"result\"") {
            return Err("released verification did not report completion".into());
        }
        // Publication retains both the latest record and immutable history.
        let published: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_file_records WHERE file_id=$1 AND record_kind='latest'",
        )
        .bind(&file)
        .fetch_one(&owner)
        .await?;
        if published != 1 {
            return Err("released verification must publish one native identity".into());
        }
        owner.close().await;
        return Ok(());
    }
    drop(socket);
    // Let HTTP transport observe the closed peer while source I/O is still held.
    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.release.notify_one();
    let exclusive = tokio::time::timeout(
        Duration::from_secs(10),
        crate::maintenance_barrier::postgres_exclusive_work_pool(&database),
    )
    .await
    .map_err(|error| std::io::Error::other(format!("release disconnected ownership: {error}")))??;
    let published: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id=$1")
            .bind(&file)
            .fetch_one(&owner)
            .await?;
    if published != 0 {
        return Err("disconnected verification published a record".into());
    }
    exclusive.close().await;
    proxy.release.notify_one();
    let response = client
        .post(format!("{base}/v2/shards"))
        .bearer_auth(&token)
        .body(shard)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if !response.contains("\"type\":\"result\"") {
        return Err("retry did not complete".into());
    }
    let retried: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id=$1")
            .bind(&file)
            .fetch_one(&owner)
            .await?;
    if retried == 0 {
        return Err("retry did not publish the expected native identity".into());
    }
    owner.close().await;
    Ok(())
}
