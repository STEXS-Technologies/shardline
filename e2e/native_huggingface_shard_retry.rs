//! Unmodified hf_xet retries a real, rolled-back PostgreSQL shard publication.
use super::*;
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Events {
    v2: AtomicUsize,
    v1: AtomicUsize,
    retryable: AtomicUsize,
    failed: AtomicUsize,
    completed: AtomicUsize,
}

#[derive(Clone)]
struct FrontDoor {
    upstream: String,
    client: reqwest::Client,
    events: Arc<Events>,
}

struct Proxy(JoinHandle<()>);
impl Drop for Proxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn forward(State(state): State<FrontDoor>, request: Request) -> Result<Response, StatusCode> {
    let v2 = request.method() == reqwest::Method::POST && request.uri().path() == "/v2/shards";
    if v2 {
        state.events.v2.fetch_add(1, Ordering::SeqCst);
    }
    if request.method() == reqwest::Method::POST
        && matches!(request.uri().path(), "/v1/shards" | "/shards")
    {
        state.events.v1.fetch_add(1, Ordering::SeqCst);
    }
    let target = format!("{}{}", state.upstream, request.uri());
    let (mut parts, body) = request.into_parts();
    for name in [header::HOST, header::CONNECTION, header::TRANSFER_ENCODING] {
        parts.headers.remove(name);
    }
    let mut upstream = state
        .client
        .request(parts.method, target)
        .headers(parts.headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let status = upstream.status();
    let mut headers = upstream.headers().clone();
    headers.remove(header::CONNECTION);
    headers.remove(header::TRANSFER_ENCODING);
    let body = if v2 {
        // Only small progress responses are buffered, never uploaded/downloaded content.
        // Forward every event unchanged; the stock client decides whether to retry.
        let mut bytes = Vec::new();
        while let Some(chunk) = upstream
            .chunk()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
        {
            if bytes.len().saturating_add(chunk.len()) > 65536 {
                return Err(StatusCode::BAD_GATEWAY);
            }
            bytes.extend_from_slice(&chunk);
        }
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let event: serde_json::Value =
                serde_json::from_slice(line).map_err(|_| StatusCode::BAD_GATEWAY)?;
            if event["type"] == "error" && event["retryable"] == true {
                state.events.retryable.fetch_add(1, Ordering::SeqCst);
            }
            if event["type"] == "error" {
                state.events.failed.fetch_add(1, Ordering::SeqCst);
            }
            if event["type"] == "result" {
                state.events.completed.fetch_add(1, Ordering::SeqCst);
            }
        }
        Body::from(bytes)
    } else {
        Body::from_stream(upstream.bytes_stream())
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

async fn interrupt_first_publication(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
    tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar(
                "SELECT pid FROM pg_locks WHERE locktype='advisory' AND NOT granted
                 AND classid=731921 AND objid=9017 AND objsubid=2",
            ).fetch_optional(&pool).await?;
            if let Some(pid) = pid {
                let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                    .bind(pid).fetch_one(&pool).await?;
                assert!(killed, "terminate the blocked publication owner");
                let records: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_file_records")
                    .fetch_one(&pool).await?;
                assert_eq!(records, 0, "the interrupted attempt must not publish records");
                sqlx::raw_sql("DROP TRIGGER shard_retry_cut ON shardline_file_records; DROP FUNCTION public.shard_retry_cut();")
                    .execute(&pool).await?;
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }).await.map_err(|_| sqlx::Error::Protocol("native shard publication never reached the owned cut".into()))?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires stock hf_xet v2 client and owned PostgreSQL/MinIO"]
async fn native_huggingface_retries_streamed_shard_owner_loss() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "native retry qualification requires hf"
    );
    assert!(
        python_package_available("hf_xet"),
        "native retry qualification requires hf_xet"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("Docker required")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let public = format!("http://{}", listener.local_addr()?);
    let mut runtime =
        start_runtime_with_roles(&[ServerFrontend::Hub], Some(services), Some(&public), false)
            .await?;
    let pool = sqlx::PgPool::connect(
        runtime
            .postgres_url
            .as_deref()
            .ok_or("PostgreSQL required")?,
    )
    .await?;
    let events = Arc::new(Events::default());
    let router = Router::new().fallback(any(forward)).with_state(FrontDoor {
        upstream: runtime.base_url.clone(),
        events: events.clone(),
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(120))
            .build()?,
    });
    let _proxy = Proxy(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    runtime.base_url = public;
    let mut blocker = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(731921,9017)")
        .execute(&mut *blocker)
        .await?;
    sqlx::raw_sql(
        "CREATE FUNCTION public.shard_retry_cut() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_advisory_xact_lock(731921,9017); RETURN NEW; END $$;
        CREATE TRIGGER shard_retry_cut BEFORE INSERT ON shardline_file_records
        FOR EACH ROW EXECUTE FUNCTION public.shard_retry_cut();",
    )
    .execute(&pool)
    .await?;
    let working = tempfile::tempdir()?;
    let source = working.path().join("retry.bin");
    let payload: Vec<u8> = (0..9 * 1024 * 1024)
        .map(|index| ((index * 31 + index / 251) % 256) as u8)
        .collect();
    write(&source, &payload)?;
    let home = tempfile::tempdir()?;
    let source_path = path_as_str(&source)?;
    let cut_pool = pool.clone();
    let cut = tokio::spawn(async move {
        let result = interrupt_first_publication(cut_pool).await;
        // Release even on a failed cut so the client cannot wait indefinitely.
        blocker.rollback().await?;
        result
    });
    let upload = run_hf(
        &runtime,
        home.path(),
        [
            "upload",
            "team/cli-model",
            source_path,
            "retry.bin",
            "--token",
            &runtime.model_token,
        ],
    );
    let cut_result = cut.await?;
    cut_result?;
    assert!(
        upload.is_ok(),
        "native upload failed: v2={}, v1={}, errors={}, retryable={}, completed={}",
        events.v2.load(Ordering::SeqCst),
        events.v1.load(Ordering::SeqCst),
        events.failed.load(Ordering::SeqCst),
        events.retryable.load(Ordering::SeqCst),
        events.completed.load(Ordering::SeqCst)
    );
    assert_eq!(
        events.v2.load(Ordering::SeqCst),
        2,
        "one failed v2 attempt and one retry"
    );
    assert_eq!(
        events.v1.load(Ordering::SeqCst),
        0,
        "no v1 fallback masks this failure"
    );
    assert_eq!(events.retryable.load(Ordering::SeqCst), 1);
    assert_eq!(events.failed.load(Ordering::SeqCst), 1);
    assert_eq!(events.completed.load(Ordering::SeqCst), 1);
    let cold = tempfile::tempdir()?;
    let download = working.path().join("download");
    run_hf(
        &runtime,
        cold.path(),
        [
            "download",
            "team/cli-model",
            "retry.bin",
            "--local-dir",
            path_as_str(&download)?,
            "--force-download",
            "--token",
            &runtime.model_token,
        ],
    )?;
    assert_eq!(read(download.join("retry.bin"))?, payload);
    pool.close().await;
    Ok(())
}
