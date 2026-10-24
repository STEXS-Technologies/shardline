//! Real Hub control plane and Git transport; no mock forge or LFS negotiator.
use super::*;
use shardline_protocol::{RepositoryScope, TokenClaims};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Server(tokio::task::JoinHandle<Result<(), std::io::Error>>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_git_xet_push_and_cold_clone_use_the_real_hub_control_plane() -> Result<(), TestError>
{
    exercise(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO plus real git-xet"]
async fn stock_git_xet_real_hub_with_postgres_and_minio() -> Result<(), TestError> {
    assert!(
        command_available("git") && command_available("git-lfs") && command_available("git-xet")
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote git-xet qualification requires Docker")?;
    exercise(Some(services)).await
}

async fn exercise(
    services: Option<shardline_test_support::DockerLocalStack>,
) -> Result<(), TestError> {
    if !(command_available("git") && command_available("git-lfs") && command_available("git-xet")) {
        return Ok(());
    }
    let storage = tempfile::tempdir()?;
    let writer = tempfile::tempdir()?;
    let writer_home = tempfile::tempdir()?;
    let reader = tempfile::tempdir()?;
    let reader_home = tempfile::tempdir()?;
    let key = b"test-signing-key-32-bytes-long!!";
    let credential = LocalHmacProvider::new(key)?.mint_token(&TokenClaims::new(
        "local",
        "native-hub",
        TokenScope::Write,
        RepositoryScope::new(RepositoryProvider::Generic, "team", "assets", None)?,
        u64::MAX,
    )?)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let base = format!("http://{address}");
    // git-xet obtains its Hub bearer token through Git's credential helper;
    // Git's http.extraHeader alone is not inherited by that token exchange.
    // Keep credentials in disposable, private client homes, never in argv.
    for home in [writer_home.path(), reader_home.path()] {
        let credentials = home.join(".git-credentials");
        write_file(
            &credentials,
            format!("http://native:{credential}@{address}\n").as_bytes(),
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600))?;
        }
        run_git_with_home(
            home,
            home,
            ["config", "--global", "credential.helper", "store"],
        )?;
    }
    let mut config = ServerConfig::new(
        address,
        base.clone(),
        storage.path().to_owned(),
        NonZeroUsize::new(65536).ok_or("chunk size")?,
    )
    .with_token_signing_key(key.to_vec())?
    .with_server_frontends([shardline_server::ServerFrontend::Hub])?
    .with_hub_webhook_secret_key(vec![7; 32])?
    .with_deployment_mode(shardline_server::DeploymentMode::Insecure);
    if let Some(stack) = &services {
        let url = stack
            .postgres_url()
            .ok_or("remote git-xet PostgreSQL missing")?;
        let pool = sqlx::PgPool::connect(&url).await?;
        shardline_server::apply_database_migrations(&pool).await?;
        pool.close().await;
        let prefix = stack.unique_s3_key_prefix("native-git-xet-hub");
        let raw = stack
            .s3_raw_config(Some(&prefix))
            .ok_or("remote git-xet MinIO missing")?;
        let objects = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                raw.session_token.map(shardline_protocol::SecretString::new),
            )
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_allow_http(raw.allow_http);
        config = config
            .with_index_postgres_url(url)?
            .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(objects));
    }
    let (router, _background) = shardline_server::app::router_with_background_tasks(config).await?;
    let xorbs = Arc::new(AtomicUsize::new(0));
    let shards = Arc::new(AtomicUsize::new(0));
    let basic = Arc::new(AtomicUsize::new(0));
    let downloads = Arc::new(AtomicUsize::new(0));
    let counts = (
        xorbs.clone(),
        shards.clone(),
        basic.clone(),
        downloads.clone(),
    );
    let router = router.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let counts = counts.clone();
            async move {
                let path = request.uri().path();
                if request.method() == Method::POST && path.starts_with("/v1/xorbs/") {
                    counts.0.fetch_add(1, Ordering::Relaxed);
                }
                if request.method() == Method::POST
                    && matches!(path, "/shards" | "/v1/shards" | "/v2/shards")
                {
                    counts.1.fetch_add(1, Ordering::Relaxed);
                }
                if request.method() == Method::PUT && path.contains("/lfs/") {
                    counts.2.fetch_add(1, Ordering::Relaxed);
                }
                if request.method() == Method::GET && path.starts_with("/lfs/objects/") {
                    counts.3.fetch_add(1, Ordering::Relaxed);
                }
                next.run(request).await
            }
        },
    ));
    let _server = Server(tokio::spawn(
        async move { axum::serve(listener, router).await },
    ));
    let created = Client::new()
        .post(format!("{base}/api/repos/create"))
        .bearer_auth(&credential)
        .json(&json!({"type":"model", "name":"team/assets", "private":true}))
        .send()
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let remote = format!("{base}/models/team/assets.git");
    initialize_git_xet_push_worktree(
        writer.path(),
        writer_home.path(),
        &remote,
        &format!("{remote}/info/lfs"),
    )?;
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        [
            "config",
            "http.extraHeader",
            &format!("Authorization: Bearer {credential}"),
        ],
    )?;
    // Hub creation already published its initial commit. Build on that history
    // rather than trying to overwrite it with an unrelated local root commit.
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        ["fetch", "origin", "main"],
    )?;
    run_git_with_home(
        writer.path(),
        writer_home.path(),
        ["checkout", "-B", "main", "FETCH_HEAD"],
    )?;
    let expected = seeded_bytes(9 * 1024 * 1024, 67);
    write_commit_and_push_asset(
        writer.path(),
        writer_home.path(),
        &expected,
        "native Hub Xet upload",
    )?;
    assert!(
        xorbs.load(Ordering::Relaxed) > 0,
        "real Hub negotiation must select native Xorb upload"
    );
    assert!(
        shards.load(Ordering::Relaxed) > 0,
        "native shard publication must complete"
    );
    assert_eq!(
        basic.load(Ordering::Relaxed),
        0,
        "basic LFS body upload must not replace Xet"
    );
    let oid = hex::encode(Sha256::digest(&expected));
    let repeated = Client::new()
        .post(format!("{remote}/info/lfs/objects/batch"))
        .bearer_auth(&credential)
        .json(&json!({
            "operation": "upload", "transfers": ["xet"],
            "objects": [{"oid": oid, "size": expected.len()}],
            "ref": {"name": "main"}
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(repeated["transfer"], "xet");
    assert!(
        repeated["objects"][0]["actions"].is_null(),
        "verified native publication must make the identical upload skippable"
    );
    let downloads_before = downloads.load(Ordering::Relaxed);
    create_dir_all(reader_home.path().join(".config"))?;
    let mut install = Command::new("git-xet");
    run_command_checked(
        with_test_home(&mut install, reader_home.path()).args(["install"]),
        "cold git-xet install",
    )?;
    let destination = reader.path().join("clone");
    let mut clone = Command::new("git");
    let model_remote = format!("{base}/team/assets.git");
    run_command_checked(
        with_test_home(&mut clone, reader_home.path())
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.extraHeader")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Bearer {credential}"),
            )
            .args(["clone", &model_remote])
            .arg(&destination),
        "cold Hub git-xet clone",
    )?;
    assert_eq!(read(destination.join("asset.bin"))?, expected);
    // Stock git-xet implements upload only and delegates downloads to Git LFS.
    // Serve native publication here without requiring a duplicate basic upload.
    assert!(
        downloads.load(Ordering::Relaxed) > downloads_before,
        "cold clone must download the native publication through standard Git LFS"
    );
    run_git_with_home(&destination, reader_home.path(), ["fsck", "--strict"])?;
    Ok(())
}
