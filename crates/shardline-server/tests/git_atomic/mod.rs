//! Native Git through ordinary server startup, durable backends and restart.
use super::*;

mod connection_loss;
mod lost_ack;

async fn git(directory: &Path, arguments: &[&str]) -> std::process::Output {
    let directory = directory.to_owned();
    let arguments: Vec<_> = arguments
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(arguments)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_TRACE_PACKET", "1")
            .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
            .env("GIT_HTTP_LOW_SPEED_TIME", "10")
            .output()
            .expect("native Git must be installed")
    })
    .await
    .unwrap()
}

async fn success(directory: &Path, arguments: &[&str]) -> std::process::Output {
    let output = git(directory, arguments).await;
    assert!(
        output.status.success(),
        "native Git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_atomic_git_survives_server_restart() {
    exercise(None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn native_atomic_git_on_postgres_survives_server_restart() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise(Some(&database), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PostgreSQL and owned TEST_S3_* MinIO storage"]
async fn native_atomic_git_on_postgres_and_minio_survives_server_restart() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let storage = S3ObjectStoreConfig::new(
        std::env::var("TEST_S3_BUCKET").unwrap(),
        "us-east-1".to_owned(),
    )
    .with_endpoint(Some(std::env::var("TEST_S3_ENDPOINT").unwrap()))
    .with_allow_http(true)
    .with_credentials(
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_ACCESS_KEY").unwrap(),
        )),
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_SECRET_KEY").unwrap(),
        )),
        None,
    )
    .with_key_prefix(Some(&format!(
        "atomic-git-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    )));
    exercise(Some(&database), Some(storage)).await;
}

async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let repository = format!(
        "atomic/native-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let client = Client::new();
    let server = start_with_storage(
        root.path(),
        ServerFrontend::Hub,
        false,
        database,
        storage.clone(),
    )
    .await;
    let created = client
        .post(format!("{}/api/repos/create", server.base))
        .json(&json!({"type":"model", "name":repository}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let url = format!("{}/models/{repository}", server.base);
    success(work.path(), &["clone", &url, "checkout"]).await;
    let checkout = work.path().join("checkout");
    std::fs::write(
        checkout.join("payload.txt"),
        b"first durable atomic payload\n",
    )
    .unwrap();
    success(&checkout, &["add", "payload.txt"]).await;
    success(
        &checkout,
        &[
            "-c",
            "user.name=Atomic Test",
            "-c",
            "user.email=atomic@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "First",
        ],
    )
    .await;
    let first = success(&checkout, &["rev-parse", "HEAD"]).await.stdout;
    let first_head = std::str::from_utf8(&first).unwrap().trim();
    let push = success(
        &checkout,
        &[
            "push",
            "--atomic",
            "origin",
            "HEAD:main",
            "HEAD:peer",
            "HEAD:refs/tags/atomic",
        ],
    )
    .await;
    let trace = String::from_utf8_lossy(&push.stderr);
    assert!(
        trace
            .lines()
            .any(|line| line.contains("> ") && line.contains(" atomic")),
        "{trace}"
    );
    std::fs::write(
        checkout.join("payload.txt"),
        b"second durable atomic payload\n",
    )
    .unwrap();
    success(&checkout, &["add", "payload.txt"]).await;
    success(
        &checkout,
        &[
            "-c",
            "user.name=Atomic Test",
            "-c",
            "user.email=atomic@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "Second",
        ],
    )
    .await;
    if let Some(database) = database {
        connection_loss::reject_interrupted_batch(database, &repository, &checkout, first_head)
            .await;
    }
    lost_ack::commit_without_delivering_status(&server.base, &repository, &checkout).await;
    success(
        &checkout,
        &["push", "--atomic", "origin", "HEAD:main", "HEAD:peer"],
    )
    .await;
    let head = success(&checkout, &["rev-parse", "HEAD"]).await.stdout;
    let head = std::str::from_utf8(&head).unwrap().trim();
    let rejected = git(
        &checkout,
        &["push", "--atomic", "origin", "HEAD:must-not-exist", ":main"],
    )
    .await;
    assert!(!rejected.status.success());
    let trace = String::from_utf8_lossy(&rejected.stderr);
    assert!(trace.contains("ng refs/heads/main"), "{trace}");
    assert!(trace.contains("ng refs/heads/must-not-exist"), "{trace}");
    assert!(
        success(
            &checkout,
            &["ls-remote", "origin", "refs/heads/must-not-exist"]
        )
        .await
        .stdout
        .is_empty()
    );
    server.stop().await;
    let server =
        start_with_storage(root.path(), ServerFrontend::Hub, false, database, storage).await;
    let url = format!("{}/models/{repository}", server.base);
    success(work.path(), &["clone", &url, "after-restart"]).await;
    let reopened = work.path().join("after-restart");
    assert_eq!(
        std::fs::read(reopened.join("payload.txt")).unwrap(),
        b"second durable atomic payload\n"
    );
    assert_eq!(
        success(&reopened, &["rev-parse", "HEAD"]).await.stdout,
        format!("{head}\n").as_bytes()
    );
    assert_eq!(
        success(&reopened, &["show", &format!("{first_head}:payload.txt")])
            .await
            .stdout,
        b"first durable atomic payload\n"
    );
    let peer = success(&reopened, &["ls-remote", "origin", "refs/heads/peer"]).await;
    assert_eq!(
        std::str::from_utf8(&peer.stdout).unwrap(),
        format!("{head}\trefs/heads/peer\n")
    );
    success(&reopened, &["fsck", "--strict"]).await;
    success(
        &reopened,
        &["push", "--atomic", "origin", ":peer", ":refs/tags/atomic"],
    )
    .await;
    for reference in ["refs/heads/peer", "refs/tags/atomic"] {
        assert!(
            success(&reopened, &["ls-remote", "origin", reference])
                .await
                .stdout
                .is_empty()
        );
    }
    let deleted = client
        .delete(format!("{}/api/repos/delete", server.base))
        .json(&json!({"type":"model", "name":repository}))
        .send()
        .await
        .unwrap();
    assert!(
        deleted.status().is_success(),
        "repository fixture cleanup failed: {}",
        deleted.status()
    );
    server.stop().await;
}
