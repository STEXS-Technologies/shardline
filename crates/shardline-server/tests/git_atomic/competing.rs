//! Two stock Git clients prepare conflicting atomic pushes against separate nodes.
use super::*;
use axum::{body::Body, extract::State, http::Request, response::Response};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct RaceProxy {
    backend: String,
    client: Client,
    gate: Arc<tokio::sync::Barrier>,
    posts: Arc<AtomicUsize>,
}

async fn forward(State(proxy): State<RaceProxy>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
    if parts.method == axum::http::Method::POST && parts.uri.path().ends_with("/git-receive-pack") {
        proxy.posts.fetch_add(1, Ordering::SeqCst);
        tokio::time::timeout(std::time::Duration::from_secs(15), proxy.gate.wait())
            .await
            .expect("both pushes must reach publication with the same advertised old refs");
    }
    let upstream = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.backend, parts.uri))
        .headers(parts.headers)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut response = Response::new(Body::from(upstream.bytes().await.unwrap()));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn commit(checkout: &Path, bytes: &[u8]) -> String {
    std::fs::write(checkout.join("payload.txt"), bytes).unwrap();
    success(checkout, &["add", "payload.txt"]).await;
    success(
        checkout,
        &["-c", "commit.gpgsign=false", "commit", "-m", "Contender"],
    )
    .await;
    String::from_utf8(success(checkout, &["rev-parse", "HEAD"]).await.stdout)
        .unwrap()
        .trim()
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and stock Git"]
async fn native_atomic_git_competing_replicas_with_owned_postgres_and_minio() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("start disposable Git backends")
        .expect("Docker is required");
    let database = services.postgres_url().unwrap();
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let prefix = services.unique_s3_key_prefix("git-atomic-race");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let storage = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(true)
        .with_key_prefix(Some(&prefix))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    exercise(&database, storage).await;
    pool.close().await;
}

async fn exercise(database: &str, storage: S3ObjectStoreConfig) {
    let left_root = tempfile::tempdir().unwrap();
    let right_root = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let repository = "atomic/competing-replicas";
    let client = Client::new();
    let left = start_with_storage(
        left_root.path(),
        ServerFrontend::Hub,
        false,
        Some(database),
        Some(storage.clone()),
    )
    .await;
    let right = start_with_storage(
        right_root.path(),
        ServerFrontend::Hub,
        false,
        Some(database),
        Some(storage.clone()),
    )
    .await;
    assert_eq!(
        client
            .post(format!("{}/api/repos/create", left.base))
            .json(&json!({"type":"model", "name":repository}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    let left_url = format!("{}/models/{repository}", left.base);
    let right_url = format!("{}/models/{repository}", right.base);
    success(work.path(), &["clone", &left_url, "left"]).await;
    let left_checkout = work.path().join("left");
    let base = commit(&left_checkout, b"common base\n").await;
    success(
        &left_checkout,
        &[
            "push",
            "--atomic",
            "origin",
            "HEAD:main",
            "HEAD:race-left",
            "HEAD:race-right",
        ],
    )
    .await;
    success(work.path(), &["clone", &right_url, "right"]).await;
    let right_checkout = work.path().join("right");
    let left_head = commit(&left_checkout, b"left contender\n").await;
    let right_head = commit(&right_checkout, b"right contender\n").await;
    assert_ne!(left_head, right_head);
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let posts = Arc::new(AtomicUsize::new(0));
    let mut proxies = Vec::new();
    let mut endpoints = Vec::new();
    for backend in [&left.base, &right.base] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!(
            "http://{}/models/{repository}",
            listener.local_addr().unwrap()
        ));
        let proxy = RaceProxy {
            backend: backend.clone(),
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(20))
                .build()
                .unwrap(),
            gate: gate.clone(),
            posts: posts.clone(),
        };
        proxies.push(tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().fallback(forward).with_state(proxy),
            )
            .await
            .unwrap();
        }));
    }
    let left_arguments = [
        "push",
        "--atomic",
        &endpoints[0],
        "HEAD:race-left",
        "HEAD:race-right",
    ];
    let right_arguments = [
        "push",
        "--atomic",
        &endpoints[1],
        "HEAD:race-left",
        "HEAD:race-right",
    ];
    let (left_result, right_result) = tokio::join!(
        git(&left_checkout, &left_arguments),
        git(&right_checkout, &right_arguments),
    );
    for proxy in proxies {
        proxy.abort();
    }
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    assert_ne!(
        left_result.status.success(),
        right_result.status.success(),
        "exactly one complete push must win"
    );
    let rejected = if left_result.status.success() {
        &right_result
    } else {
        &left_result
    };
    let report = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        !report.contains("atomic push storage failure"),
        "loser must be rejected by ref validation"
    );
    assert!(
        !report.contains("atomic push worker failure"),
        "loser must not fail because its worker crashed"
    );
    assert!(
        report.contains("ng refs/heads/race-left"),
        "atomic loser must reject its first ref"
    );
    assert!(
        report.contains("ng refs/heads/race-right"),
        "atomic loser must reject its second ref"
    );
    let (winner, loser, payload) = if left_result.status.success() {
        (&left_head, &right_head, b"left contender\n".as_slice())
    } else {
        (&right_head, &left_head, b"right contender\n".as_slice())
    };
    for endpoint in [&left_url, &right_url] {
        let refs = success(
            work.path(),
            &[
                "ls-remote",
                endpoint,
                "refs/heads/race-left",
                "refs/heads/race-right",
                "refs/heads/main",
            ],
        )
        .await;
        let refs = String::from_utf8(refs.stdout).unwrap();
        for (reference, expected) in [
            ("main", &base),
            ("race-left", winner),
            ("race-right", winner),
        ] {
            assert!(
                refs.lines()
                    .any(|line| line == format!("{expected}\trefs/heads/{reference}"))
            );
        }
        assert!(
            !git(work.path(), &["fetch", endpoint, loser])
                .await
                .status
                .success(),
            "losing prepared history must remain unreachable"
        );
    }
    left.stop().await;
    right.stop().await;
    let reopened_root = tempfile::tempdir().unwrap();
    let reopened = start_with_storage(
        reopened_root.path(),
        ServerFrontend::Hub,
        false,
        Some(database),
        Some(storage),
    )
    .await;
    let reopened_url = format!("{}/models/{repository}", reopened.base);
    success(
        work.path(),
        &[
            "clone",
            "--branch",
            "race-left",
            &reopened_url,
            "after-race",
        ],
    )
    .await;
    let checkout = work.path().join("after-race");
    assert_eq!(
        std::fs::read(checkout.join("payload.txt")).unwrap(),
        payload
    );
    assert_eq!(
        success(&checkout, &["rev-parse", "HEAD"]).await.stdout,
        format!("{winner}\n").as_bytes()
    );
    success(&checkout, &["fsck", "--full", "--strict"]).await;
    assert!(
        client
            .delete(format!("{}/api/repos/delete", reopened.base))
            .json(&json!({"type":"model", "name":repository}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    reopened.stop().await;
}
