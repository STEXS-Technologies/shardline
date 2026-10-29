//! Executable-level FSCK qualification. Never infer shell status from report text.
#![allow(clippy::panic_in_result_fn)]

use bytes::Bytes;
use shardline_server::LocalBackend;
use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, Output},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

mod fsck_native_indexes;
mod fsck_native_members;

fn binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_shardline")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_shardline")))
}

fn scan(root: &Path) -> std::io::Result<Output> {
    scan_configured(root, None)
}

#[cfg(unix)]
fn scan_through_shell(root: &Path) -> std::io::Result<Output> {
    shardline_test_support::native_client_output(
        Command::new("/bin/sh")
            .env_clear()
            .args([
                "-c",
                "\"$1\" fsck --root \"$2\"; result=$?; exit \"$result\"",
                "fsck-wrapper",
            ])
            .arg(binary())
            .arg(root),
        std::time::Duration::from_secs(120),
    )
}

struct RemoteStorage {
    database_url: String,
    config: shardline_storage::S3ObjectStoreConfig,
    environment: Vec<(&'static str, String)>,
}

fn scan_configured(root: &Path, remote: Option<&RemoteStorage>) -> std::io::Result<Output> {
    let mut command = Command::new(binary());
    command.env_clear();
    if let Some(remote) = remote {
        command.envs(remote.environment.iter().map(|(key, value)| (*key, value)));
    }
    shardline_test_support::native_client_output(
        command.args(["fsck", "--root"]).arg(root),
        std::time::Duration::from_secs(120),
    )
}

fn assert_status(output: &Output, expected: i32) {
    assert_eq!(
        output.status.code(),
        Some(expected),
        "FSCK returned an unexpected status; captured output is withheld"
    );
}

struct RunningServer(
    tokio::task::JoinHandle<std::result::Result<(), shardline_server::ServerError>>,
);

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_native_xet_publications_have_no_fsck_findings() -> Result<()> {
    native_corpus(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_xet_fsck_with_postgres_and_minio() -> Result<()> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("Docker is required for remote FSCK qualification")?;
    let database_url = services
        .postgres_url()
        .ok_or("PostgreSQL fixture missing")?;
    let prefix = services.unique_s3_key_prefix("fsck-native");
    let raw = services
        .s3_raw_config(Some(&prefix))
        .ok_or("MinIO fixture missing")?;
    let environment = vec![
        ("SHARDLINE_INDEX_POSTGRES_URL", database_url.clone()),
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", raw.bucket.clone()),
        ("SHARDLINE_S3_REGION", raw.region.clone()),
        (
            "SHARDLINE_S3_ENDPOINT",
            raw.endpoint.clone().ok_or("endpoint")?,
        ),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
        (
            "SHARDLINE_S3_ACCESS_KEY_ID",
            raw.access_key.clone().ok_or("access key")?,
        ),
        (
            "SHARDLINE_S3_SECRET_ACCESS_KEY",
            raw.secret_key.clone().ok_or("secret key")?,
        ),
    ];
    let config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(true);
    let pool = sqlx::PgPool::connect(&database_url).await?;
    shardline_server::apply_database_migrations(&pool).await?;
    pool.close().await;
    native_corpus(Some(&RemoteStorage {
        database_url,
        config,
        environment,
    }))
    .await
}

async fn native_corpus(remote: Option<&RemoteStorage>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let base = format!("http://{address}");
    let mut config = shardline_server::ServerConfig::new(
        address,
        base.clone(),
        root.path().to_owned(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    );
    if let Some(remote) = remote {
        config = config
            .with_index_postgres_url(remote.database_url.clone())?
            .with_object_storage(
                shardline_server::ObjectStorageAdapter::S3,
                Some(remote.config.clone()),
            );
    }
    let server = RunningServer(tokio::spawn(shardline_server::serve_with_listener(
        config.clone(),
        listener,
    )));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(10))
        .ok_or("fixture startup deadline overflow")?;
    loop {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "native fixture server did not start"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let left = b"native FSCK shared member";
    let right = b"native FSCK additional member";
    let mut hashes = Vec::new();
    for content in [left.as_slice(), right.as_slice()] {
        let (bytes, hash) = shardline_server::test_fixtures::single_chunk_xorb(content);
        client
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .body(bytes)
            .send()
            .await?
            .error_for_status()?;
        hashes.push(hash);
    }
    let left_hash = hashes.first().ok_or("missing first Xorb hash")?;
    let right_hash = hashes.get(1).ok_or("missing second Xorb hash")?;
    let mut files = Vec::new();
    for parts in [
        vec![
            (left.as_slice(), left_hash.as_str()),
            (right.as_slice(), right_hash.as_str()),
        ],
        vec![(left.as_slice(), left_hash.as_str())],
    ] {
        let (bytes, file) = shardline_server::test_fixtures::single_file_shard(&parts);
        client
            .post(format!("{base}/v1/shards"))
            .body(bytes)
            .send()
            .await?
            .error_for_status()?;
        files.push((
            file,
            parts
                .into_iter()
                .flat_map(|(member_bytes, _)| member_bytes.iter().copied())
                .collect::<Vec<_>>(),
        ));
    }
    for (file, _) in &files {
        client
            .get(format!("{base}/v1/reconstructions/{file}"))
            .send()
            .await?
            .error_for_status()?;
    }
    let packed_members = fsck_native_members::publish(&client, &base).await?;
    drop(server);
    verify_native_files(&config, &files).await?;
    let report = scan_configured(root.path(), remote)?;
    assert_status(&report, 0);
    assert!(String::from_utf8_lossy(&report.stdout).contains("issue_count: 0"));
    assert!(!String::from_utf8_lossy(&report.stderr).contains("issue:"));

    // Copy-free ingestion need not create a standalone member directory.
    // Materialize a valid legacy copy explicitly, then prove a same-length
    // mutation is detected even though the intact container serves the file.
    let member = shardline_server::test_fixtures::xet_hash_hex(
        &shardline_xet_core::merklehash::compute_data_hash(left),
    );
    let member_path = root.path().join("chunks").join(&member[..2]).join(&member);
    if remote.is_none() {
        fs::create_dir_all(member_path.parent().ok_or("member directory")?)?;
        fs::write(&member_path, left)?;
        assert_status(&scan_configured(root.path(), remote)?, 0);
    }
    if let Some(remote) = remote {
        overwrite_remote_member(remote, &member, &vec![b'!'; left.len()])?;
    } else {
        fs::write(&member_path, vec![b'!'; left.len()])?;
    }
    let damaged = scan_configured(root.path(), remote)?;
    assert_status(&damaged, 1);
    assert!(String::from_utf8_lossy(&damaged.stderr).contains("chunk_hash_mismatch"));
    if let Some(remote) = remote {
        overwrite_remote_member(remote, &member, left)?;
    } else {
        fs::write(member_path, left)?;
    }
    let restored = scan_configured(root.path(), remote)?;
    assert_status(&restored, 0);
    assert!(String::from_utf8_lossy(&restored.stdout).contains("issue_count: 0"));
    if let Some(remote) = remote {
        fsck_native_indexes::qualify(root.path(), remote, left_hash, right_hash, &member, left)
            .await?;
    }
    fsck_native_members::qualify(root.path(), remote, &packed_members)?;
    verify_native_files(&config, &files).await?;
    Ok(())
}

async fn verify_native_files(
    config: &shardline_server::ServerConfig,
    files: &[(String, Vec<u8>)],
) -> Result<()> {
    let backend = shardline_server::ServerBackend::from_config(config).await?;
    for (file, expected) in files {
        let downloaded = match &backend {
            shardline_server::ServerBackend::Local(backend) => {
                backend.download_file(file, None, None).await?
            }
            shardline_server::ServerBackend::Postgres(backend) => {
                backend.download_file(file, None, None).await?
            }
        };
        assert_eq!(&downloaded, expected);
    }
    Ok(())
}

fn overwrite_remote_member(remote: &RemoteStorage, hash: &str, bytes: &[u8]) -> Result<()> {
    use shardline_storage::{ObjectBody, ObjectIntegrity};
    let store = shardline_server_core::ServerObjectStore::s3(remote.config.clone())?;
    let key = shardline_server_core::chunk_object_key(hash)?;
    store.put_overwrite(
        &key,
        ObjectBody::from_slice(bytes),
        &ObjectIntegrity::new(
            shardline_server_core::chunk_hash(bytes),
            u64::try_from(bytes.len())?,
        ),
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_executable_distinguishes_clean_findings_and_operational_failure() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("data");
    let backend = LocalBackend::new(
        root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    let content =
        Bytes::from_static(b"fsck shell status must describe the actual integrity result");
    backend.upload_file("asset.bin", content, None).await?;

    let healthy = scan(&root)?;
    assert_status(&healthy, 0);
    #[cfg(unix)]
    assert_status(&scan_through_shell(&root)?, 0);
    assert!(String::from_utf8_lossy(&healthy.stdout).contains("issue_count: 0"));
    assert!(!String::from_utf8_lossy(&healthy.stderr).contains("issue:"));

    // Hide the immutable objects without changing valid metadata or evidence.
    // Restore them afterward to prove the finding describes object availability.
    let retained = sandbox.path().join("retained-chunks");
    fs::rename(root.join("chunks"), &retained)?;
    fs::create_dir(root.join("chunks"))?;
    let missing = scan(&root)?;
    assert_status(&missing, 1);
    #[cfg(unix)]
    assert_status(&scan_through_shell(&root)?, 1);
    assert!(!String::from_utf8_lossy(&missing.stdout).contains("issue_count: 0"));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("issue:"));
    fs::remove_dir(root.join("chunks"))?;
    fs::rename(retained, root.join("chunks"))?;
    let restored = scan(&root)?;
    assert_status(&restored, 0);
    assert!(String::from_utf8_lossy(&restored.stdout).contains("issue_count: 0"));

    // A regular file cannot be a deployment directory; this is a scan failure,
    // not a completed scan with zero findings or an invented corruption report.
    let invalid_root = sandbox.path().join("not-a-directory");
    fs::write(&invalid_root, b"not a deployment")?;
    let failed = scan(&invalid_root)?;
    assert_status(&failed, 2);
    #[cfg(unix)]
    assert_status(&scan_through_shell(&invalid_root)?, 2);
    assert!(!failed.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&failed.stdout).contains("issue_count: 0"));
    Ok(())
}
