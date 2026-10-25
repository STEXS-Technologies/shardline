//! Kill the actual server after atomic publication but before Git sees its reply.
use super::*;
use axum::{body::Body, extract::State, http::Request, response::Response};
use std::sync::Arc;
use tokio::sync::Notify;

#[cfg(feature = "docker")]
mod pending;

#[derive(Clone, Copy)]
enum Cut {
    AfterCommit,
    #[cfg(feature = "docker")]
    BeforeCommit,
}

#[derive(Clone)]
struct PublicationProxy {
    backend: String,
    client: reqwest::Client,
    committed: Arc<Notify>,
    release: Arc<Notify>,
}

async fn forward(State(proxy): State<PublicationProxy>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let publication =
        parts.method == axum::http::Method::POST && parts.uri.path().ends_with("/git-receive-pack");
    let body = axum::body::to_bytes(body, 8 * 1024 * 1024).await.unwrap();
    let Ok(upstream) = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.backend, parts.uri))
        .headers(parts.headers)
        .body(body)
        .send()
        .await
    else {
        // A killed server can close the socket before returning headers. Do
        // not panic or print a transport error containing credentials or URLs.
        return Response::builder().status(502).body(Body::empty()).unwrap();
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let bytes = upstream.bytes().await.unwrap();
    if publication {
        assert_eq!(status, reqwest::StatusCode::OK);
        let report = String::from_utf8_lossy(&bytes);
        for reference in ["main", "peer"] {
            assert!(
                report.contains(&format!("ok refs/heads/{reference}")),
                "server did not acknowledge the complete atomic batch"
            );
        }
        proxy.committed.notify_one();
        proxy.release.notified().await;
        return Response::new(Body::from_stream(futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"000")),
            Err(std::io::Error::other("injected lost publication response")),
        ])));
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn git(root: &Path, token: &str, args: &[&str]) -> std::process::Output {
    let root = root.to_owned();
    let token = token.to_owned();
    let args: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        shardline_test_support::native_client_output(
            Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                .env(
                    "GIT_CONFIG_VALUE_0",
                    format!("Authorization: Bearer {token}"),
                )
                .env("GIT_AUTHOR_NAME", "Publication Test")
                .env("GIT_AUTHOR_EMAIL", "publication@example.invalid")
                .env("GIT_COMMITTER_NAME", "Publication Test")
                .env("GIT_COMMITTER_EMAIL", "publication@example.invalid"),
            Duration::from_secs(30),
        )
        .expect("Git must finish within its deadline")
    })
    .await
    .unwrap()
}

async fn success(root: &Path, token: &str, args: &[&str]) -> Vec<u8> {
    let output = git(root, token, args).await;
    assert!(
        output.status.success(),
        "Git command failed: {}",
        output.status
    );
    output.stdout
}

#[tokio::test]
#[ignore = "requires actual Shardline executable and stock Git"]
async fn atomic_git_survives_sigkill_after_commit_before_ack() {
    exercise(None, &[], Cut::AfterCommit).await;
}

#[cfg(feature = "docker")]
#[tokio::test]
#[ignore = "requires actual executable, stock Git and owned PostgreSQL/MinIO"]
async fn remote_atomic_git_survives_sigkill_after_commit_before_ack() {
    remote(Cut::AfterCommit).await;
}

#[cfg(feature = "docker")]
#[tokio::test]
#[ignore = "requires actual executable, stock Git and owned PostgreSQL/MinIO"]
async fn remote_atomic_git_rolls_back_sigkill_during_second_ref() {
    remote(Cut::BeforeCommit).await;
}

#[cfg(feature = "docker")]
async fn remote(cut: Cut) {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .unwrap()
        .expect("Docker is required");
    let database = services.postgres_url().unwrap();
    let prefix = services.unique_s3_key_prefix("git-process-cut");
    let s3 = services.s3_raw_config(Some(&prefix)).unwrap();
    let storage = vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", s3.bucket),
        ("SHARDLINE_S3_REGION", s3.region),
        ("SHARDLINE_S3_ENDPOINT", s3.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", s3.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", s3.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    exercise(Some(&database), &storage, cut).await;
}

async fn exercise(database: Option<&str>, storage: &[(&str, String)], cut: Cut) {
    let binary = resolve_shardline_binary().expect("actual executable required");
    let root = TempDir::new().unwrap();
    let work = TempDir::new().unwrap();
    let mut storage = storage.to_vec();
    storage.push(("SHARDLINE_HUB_WEBHOOK_SECRET_KEY", "00".repeat(16)));
    migrate_metadata(&binary, root.path(), database).await;
    let mut server =
        LocalServer::spawn_with_storage(&binary, root.path(), database, "hub", &storage);
    server.ready().await;
    // This batch updates two branches. The usual deployment helper deliberately
    // binds its credential to main and must not authorize creation of peer.
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "drill", "drill", None).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "git-process-cut",
        TokenScope::Write,
        repository,
        u64::MAX,
    )
    .unwrap();
    let token = provider.mint_token(&claims).unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let response = client
        .post(format!("{}/api/repos/create", server.base))
        .bearer_auth(&token)
        .json(&serde_json::json!({"type":"model", "name":"drill/drill"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let initial_url = format!("{}/models/drill/drill", server.base);
    success(work.path(), &token, &["clone", &initial_url, "checkout"]).await;
    let checkout_path = work.path().join("checkout");
    std::fs::write(
        checkout_path.join("payload.txt"),
        b"durable multi-ref publication\n",
    )
    .unwrap();
    success(&checkout_path, &token, &["add", "payload.txt"]).await;
    success(
        &checkout_path,
        &token,
        &[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "atomic payload",
        ],
    )
    .await;
    let head = success(&checkout_path, &token, &["rev-parse", "HEAD"]).await;
    let head = std::str::from_utf8(&head).unwrap().trim().to_owned();
    #[cfg(feature = "docker")]
    let pending = match cut {
        Cut::BeforeCommit => Some(pending::Publication::arm(database.unwrap()).await),
        Cut::AfterCommit => None,
    };
    let committed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let proxy = PublicationProxy {
        backend: server.base.clone(),
        client,
        committed: Arc::clone(&committed),
        release: Arc::clone(&release),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/models/drill/drill",
        listener.local_addr().unwrap()
    );
    let router = axum::Router::new().fallback(forward).with_state(proxy);
    let proxy_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let checkout = checkout_path.clone();
    let auth = token.clone();
    let push = tokio::spawn(async move {
        git(
            &checkout,
            &auth,
            &["push", "--atomic", &endpoint, "HEAD:main", "HEAD:peer"],
        )
        .await
    });
    match cut {
        Cut::AfterCommit => tokio::time::timeout(Duration::from_secs(30), committed.notified())
            .await
            .expect("server must commit both refs before the process cut"),
        #[cfg(feature = "docker")]
        Cut::BeforeCommit => pending.as_ref().unwrap().observe().await,
    }
    server.child.kill().unwrap();
    assert!(!server.child.wait().unwrap().success());
    release.notify_one();
    assert!(
        !push.await.unwrap().status.success(),
        "Git unexpectedly received publication status"
    );
    proxy_task.abort();
    let _stopped = proxy_task.await;
    drop(server);
    #[cfg(feature = "docker")]
    if let Some(pending) = pending {
        pending.verify_rollback().await;
    }
    let mut server =
        LocalServer::spawn_with_storage(&binary, root.path(), database, "hub", &storage);
    server.ready().await;
    let url = format!("{}/models/drill/drill", server.base);
    if matches!(cut, Cut::AfterCommit) {
        for reference in ["refs/heads/main", "refs/heads/peer"] {
            let refs = success(work.path(), &token, &["ls-remote", &url, reference]).await;
            assert_eq!(
                std::str::from_utf8(&refs).unwrap(),
                format!("{head}\t{reference}\n")
            );
        }
    }
    success(
        &checkout_path,
        &token,
        &["push", "--atomic", &url, "HEAD:main", "HEAD:peer"],
    )
    .await;
    success(work.path(), &token, &["clone", &url, "recovered"]).await;
    let recovered = work.path().join("recovered");
    assert_eq!(
        std::fs::read(recovered.join("payload.txt")).unwrap(),
        b"durable multi-ref publication\n"
    );
    success(&recovered, &token, &["fsck", "--strict", "--full"]).await;
}
