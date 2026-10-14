use super::super::*;
use shardline_storage::{ObjectKey, ObjectPrefix, ObjectStore};
use std::{fs, process::Command, thread, time::Instant};

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[test]
fn interrupted_local_to_s3_copy_is_inspectable_from_source_without_replay() {
    let binary = std::env::var_os("CARGO_BIN_EXE_shardline")
        .expect("set CARGO_BIN_EXE_shardline to the current optimized binary");
    let stack = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("owned MinIO fixture requires Docker");
    let prefix = stack.unique_s3_key_prefix("interrupted-storage-copy");
    let raw = stack.s3_raw_config(Some(&prefix)).unwrap();
    let objects = S3ObjectStoreConfig::new(raw.bucket.clone(), raw.region.clone())
        .with_endpoint(raw.endpoint.clone())
        .with_credentials(
            raw.access_key.clone().map(SecretString::new),
            raw.secret_key.clone().map(SecretString::new),
            raw.session_token.clone().map(SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http);
    let proxy = S3FaultProxy::start(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::DelaySuccessfulPutResponse(Duration::from_secs(3)),
    )
    .unwrap();
    let source = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    shardline_index::LocalIndexStore::new(source.path().to_path_buf()).unwrap();
    fs::create_dir_all(source.path().join("chunks/copy-cut")).unwrap();
    let bytes = b"remote copy applied before acknowledgement";
    fs::write(source.path().join("chunks/copy-cut/first"), bytes).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".to_owned(),
        source.path().to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(objects));
    let remote = shardline_server::object_store_from_config(&config).unwrap();
    let mut command = Command::new(&binary);
    command
        .env_clear()
        .env("TMPDIR", scratch.path())
        .env("SHARDLINE_MIGRATE_TO_S3_BUCKET", &raw.bucket)
        .env("SHARDLINE_MIGRATE_TO_S3_REGION", &raw.region)
        .env("SHARDLINE_MIGRATE_TO_S3_ENDPOINT", proxy.endpoint())
        .env("SHARDLINE_MIGRATE_TO_S3_KEY_PREFIX", &prefix)
        .env("SHARDLINE_MIGRATE_TO_S3_ALLOW_HTTP", "true")
        .env(
            "SHARDLINE_MIGRATE_TO_S3_ACCESS_KEY_ID",
            raw.access_key.as_deref().unwrap(),
        )
        .env(
            "SHARDLINE_MIGRATE_TO_S3_SECRET_ACCESS_KEY",
            raw.secret_key.as_deref().unwrap(),
        )
        .current_dir(source.path())
        .args(["storage", "migrate", "--from", "local", "--from-root"])
        .arg(source.path())
        .args(["--to", "s3"])
        .stdout(fs::File::create(scratch.path().join("cli.stdout")).unwrap())
        .stderr(fs::File::create(scratch.path().join("cli.stderr")).unwrap());
    let mut child = StopOnDrop(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    while !proxy.fault_was_injected() {
        assert!(
            Instant::now() < deadline,
            "remote copy did not reach the response fault"
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "{}",
            fs::read_to_string(scratch.path().join("cli.stderr")).unwrap()
        );
        thread::sleep(Duration::from_millis(5));
    }
    let key = ObjectKey::parse("copy-cut/first").unwrap();
    assert_eq!(
        remote
            .read_range(
                &key,
                shardline_protocol::ByteRange::new(0, (bytes.len() - 1) as u64).unwrap()
            )
            .unwrap(),
        bytes
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    fs::write(source.path().join("chunks/copy-cut/later"), b"never replay").unwrap();
    let history = Command::new(&binary)
        .env_clear()
        .env("TMPDIR", scratch.path())
        .current_dir(source.path())
        .args(["history", "--root"])
        .arg(source.path())
        .output()
        .unwrap();
    assert!(
        history.status.success(),
        "{}",
        String::from_utf8_lossy(&history.stderr)
    );
    let page: serde_json::Value = serde_json::from_slice(&history.stdout).unwrap();
    let tasks = page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .unwrap();
    assert_eq!(
        tasks.len(),
        1,
        "source history must expose the interrupted remote copy"
    );
    let task = tasks.first().unwrap();
    assert_eq!(
        task.get("state").and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    assert_eq!(
        task.get("owner")
            .and_then(|owner| owner.get("pid"))
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(child.0.id()))
    );
    assert!(
        remote
            .metadata(&ObjectKey::parse("copy-cut/later").unwrap())
            .unwrap()
            .is_none()
    );
    assert!(
        remote
            .list_prefix(&ObjectPrefix::parse("_shardline_tasks/").unwrap())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        remote
            .read_range(
                &key,
                shardline_protocol::ByteRange::new(0, (bytes.len() - 1) as u64).unwrap()
            )
            .unwrap(),
        bytes
    );
}
