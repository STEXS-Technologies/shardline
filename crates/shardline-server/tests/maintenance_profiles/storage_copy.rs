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
    interrupted_copy(true, true);
}

#[test]
fn interrupted_s3_to_s3_copy_is_inspectable_without_worker_setup_or_replay() {
    interrupted_copy(false, true);
}

#[test]
fn s3_only_copy_preserves_work_when_local_history_path_is_unsafe() {
    interrupted_copy(false, false);
}

fn interrupted_copy(local_source: bool, history_available: bool) {
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
    let history_root = if local_source {
        source.path().to_path_buf()
    } else {
        source.path().join(".shardline/data")
    };
    let outside = TempDir::new().unwrap();
    if history_available && local_source {
        shardline_index::LocalIndexStore::new(history_root).unwrap();
    } else if !history_available {
        std::os::unix::fs::symlink(outside.path(), source.path().join(".shardline")).unwrap();
    }
    let bytes = b"remote copy applied before acknowledgement";
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".to_owned(),
        source.path().to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(objects));
    let remote = shardline_server::object_store_from_config(&config).unwrap();
    let source_prefix = stack.unique_s3_key_prefix("copy-source");
    let source_config = config.clone().with_object_storage(
        ObjectStorageAdapter::S3,
        Some(
            config
                .s3_object_store_config()
                .unwrap()
                .clone()
                .with_key_prefix(Some(&source_prefix)),
        ),
    );
    let remote_source = shardline_server::object_store_from_config(&source_config).unwrap();
    if local_source {
        fs::create_dir_all(source.path().join("chunks/copy-cut")).unwrap();
        fs::write(source.path().join("chunks/copy-cut/first"), bytes).unwrap();
    } else {
        put_copy_source(&remote_source, "copy-cut/first", bytes);
    }
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
        .stdout(fs::File::create(scratch.path().join("cli.stdout")).unwrap())
        .stderr(fs::File::create(scratch.path().join("cli.stderr")).unwrap());
    if local_source {
        command
            .args(["storage", "migrate", "--from", "local", "--from-root"])
            .arg(source.path())
            .args(["--to", "s3"]);
    } else {
        command
            .args(["storage", "migrate", "--from", "s3", "--to", "s3"])
            .env("SHARDLINE_MIGRATE_FROM_S3_BUCKET", &raw.bucket)
            .env("SHARDLINE_MIGRATE_FROM_S3_REGION", &raw.region)
            .env(
                "SHARDLINE_MIGRATE_FROM_S3_ENDPOINT",
                raw.endpoint.as_deref().unwrap(),
            )
            .env("SHARDLINE_MIGRATE_FROM_S3_KEY_PREFIX", &source_prefix)
            .env("SHARDLINE_MIGRATE_FROM_S3_ALLOW_HTTP", "true")
            .env(
                "SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID",
                raw.access_key.as_deref().unwrap(),
            )
            .env(
                "SHARDLINE_MIGRATE_FROM_S3_SECRET_ACCESS_KEY",
                raw.secret_key.as_deref().unwrap(),
            );
    }
    let mut child = StopOnDrop(command.spawn().unwrap());
    let started = Instant::now();
    while !proxy.fault_was_injected() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
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
    let end = u64::try_from(bytes.len()).unwrap().checked_sub(1).unwrap();
    assert_eq!(
        remote
            .read_range(&key, shardline_protocol::ByteRange::new(0, end).unwrap())
            .unwrap(),
        bytes
    );
    if !history_available {
        assert!(child.0.wait().unwrap().success());
        assert!(
            fs::read_to_string(scratch.path().join("cli.stdout"))
                .unwrap()
                .contains("inserted_objects: 1")
        );
        assert!(
            fs::read_to_string(scratch.path().join("cli.stderr"))
                .unwrap()
                .contains("maintenance tracking unavailable")
        );
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
        assert!(
            fs::symlink_metadata(source.path().join(".shardline"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            remote
                .list_prefix(&ObjectPrefix::parse("_shardline_tasks/").unwrap())
                .unwrap()
                .is_empty()
        );
        return;
    }
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    if local_source {
        fs::write(source.path().join("chunks/copy-cut/later"), b"never replay").unwrap();
    } else {
        put_copy_source(&remote_source, "copy-cut/later", b"never replay");
    }
    let mut history_command = Command::new(&binary);
    history_command
        .env_clear()
        .env("TMPDIR", scratch.path())
        .current_dir(source.path())
        .arg("history");
    if local_source {
        history_command.arg("--root").arg(source.path());
    }
    let history = history_command.output().unwrap();
    if !local_source {
        assert!(
            source
                .path()
                .join(".shardline/data/metadata.sqlite3")
                .is_file()
        );
    }
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
            .read_range(&key, shardline_protocol::ByteRange::new(0, end).unwrap())
            .unwrap(),
        bytes
    );
}

fn put_copy_source(store: &shardline_server::ServerObjectStore, key: &str, bytes: &[u8]) {
    store
        .put_if_absent(
            &ObjectKey::parse(key).unwrap(),
            shardline_storage::ObjectBody::from_vec(bytes.to_vec()),
            &shardline_storage::ObjectIntegrity::new(
                shardline_server::chunk_hash(bytes),
                bytes.len() as u64,
            ),
        )
        .unwrap();
}
