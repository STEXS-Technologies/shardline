//! Native Git through ordinary server startup, durable backends and restart.
use super::*;

mod connection_loss;
mod interrupted_fetch;
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

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and stock Git"]
async fn native_atomic_git_with_owned_postgres_and_minio() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("start disposable Git backends")
        .expect("Docker is required for remote Git qualification");
    let database = services.postgres_url().expect("PostgreSQL fixture");
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let prefix = services.unique_s3_key_prefix("git-atomic");
    let raw = services
        .s3_raw_config(Some(&prefix))
        .expect("MinIO fixture");
    let storage = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(true)
        .with_key_prefix(Some(&prefix))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    exercise(Some(&database), Some(storage)).await;
    pool.close().await;
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

    // Exercise merge-shaped history over the real Smart HTTP path after the
    // server restart. A depth-two clone must receive both merge parents, and
    // deepening must restore their shared ancestor without duplicating it.
    success(&reopened, &["switch", "-c", "shallow-side", first_head]).await;
    std::fs::write(reopened.join("shallow-side.txt"), b"side branch payload\n").unwrap();
    success(&reopened, &["add", "shallow-side.txt"]).await;
    success(
        &reopened,
        &[
            "-c",
            "user.name=Atomic Test",
            "-c",
            "user.email=atomic@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "Shallow side",
        ],
    )
    .await;
    success(
        &reopened,
        &["push", "origin", "HEAD:refs/heads/shallow-side"],
    )
    .await;
    success(&reopened, &["switch", "main"]).await;
    success(
        &reopened,
        &["merge", "--no-ff", "--no-edit", "shallow-side"],
    )
    .await;
    success(&reopened, &["push", "origin", "HEAD:main"]).await;

    let shallow_path = work.path().join("shallow-merge");
    let shallow_dir = shallow_path.to_str().unwrap();
    let repository_url = format!("{}/models/{repository}", server.base);
    success(
        work.path(),
        &[
            "clone",
            "--depth=2",
            "--single-branch",
            "--branch",
            "main",
            &repository_url,
            shallow_dir,
        ],
    )
    .await;
    let shallow_head = success(&shallow_path, &["rev-list", "--parents", "-n", "1", "HEAD"])
        .await
        .stdout;
    assert_eq!(
        std::str::from_utf8(&shallow_head)
            .unwrap()
            .split_whitespace()
            .count(),
        3,
        "shallow merge clone must retain both parent identities"
    );
    assert_eq!(
        success(&shallow_path, &["rev-list", "--count", "HEAD"])
            .await
            .stdout,
        b"3\n"
    );
    success(&shallow_path, &["fsck", "--strict"]).await;
    interrupted_fetch::fail_once_then_retry(&shallow_path, &server.base, &repository).await;
    assert_eq!(
        success(&shallow_path, &["rev-list", "--count", "HEAD"])
            .await
            .stdout,
        b"4\n"
    );
    assert_eq!(
        success(
            &shallow_path,
            &["rev-parse", &format!("{first_head}^{{commit}}")]
        )
        .await
        .stdout,
        format!("{first_head}\n").as_bytes()
    );
    success(&shallow_path, &["fsck", "--strict"]).await;
    success(&shallow_path, &["fetch", "--unshallow", "origin", "main"]).await;
    assert_eq!(
        success(&shallow_path, &["rev-parse", "--is-shallow-repository"])
            .await
            .stdout,
        b"false\n"
    );
    assert!(
        git(
            &shallow_path,
            &["merge-base", "--is-ancestor", first_head, "HEAD"]
        )
        .await
        .status
        .success()
    );
    success(&shallow_path, &["fsck", "--full", "--strict"]).await;
    assert_eq!(
        success(&shallow_path, &["rev-list", "--parents", "HEAD"])
            .await
            .stdout,
        success(&reopened, &["rev-list", "--parents", "HEAD"])
            .await
            .stdout,
        "unshallowing must restore the same complete commit graph"
    );

    let filtered_path = work.path().join("filtered-merge");
    let filtered_dir = filtered_path.to_str().unwrap();
    success(
        work.path(),
        &[
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            "--single-branch",
            "--branch",
            "main",
            &repository_url,
            filtered_dir,
        ],
    )
    .await;
    assert_eq!(
        success(
            &filtered_path,
            &["config", "--get", "remote.origin.promisor"]
        )
        .await
        .stdout,
        b"true\n"
    );
    let missing_blobs = success(
        &filtered_path,
        &["rev-list", "--objects", "--all", "--missing=print"],
    )
    .await
    .stdout;
    assert!(
        String::from_utf8_lossy(&missing_blobs)
            .lines()
            .any(|line| line.starts_with('?')),
        "blob:none clone should defer at least one blob"
    );
    success(&filtered_path, &["checkout", "--detach", "main"]).await;
    assert_eq!(
        std::fs::read(filtered_path.join("payload.txt")).unwrap(),
        b"second durable atomic payload\n"
    );
    assert_eq!(
        std::fs::read(filtered_path.join("shallow-side.txt")).unwrap(),
        b"side branch payload\n"
    );
    success(&filtered_path, &["fsck", "--full", "--strict"]).await;

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
