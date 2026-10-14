use super::super::*;
use shardline_storage::{ObjectKey, ObjectPrefix, ObjectStore};
use std::os::unix::{fs::MetadataExt, process::ExitStatusExt};
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
    verify_copy_history(CopyProfile::LocalSource, CopyInterruption::KillAcceptedPut);
}

#[test]
fn interrupted_s3_to_s3_copy_is_inspectable_without_worker_setup_or_replay() {
    verify_copy_history(CopyProfile::S3Source, CopyInterruption::KillAcceptedPut);
}

#[test]
fn s3_to_s3_copy_history_is_inspectable_with_unrelated_postgres_settings() {
    verify_copy_history(
        CopyProfile::ConfiguredDatabaseS3Source,
        CopyInterruption::KillAcceptedPut,
    );
}

#[test]
fn s3_only_copy_preserves_work_when_local_history_path_is_unsafe() {
    verify_copy_history(
        CopyProfile::UnsafeHistory,
        CopyInterruption::KillAcceptedPut,
    );
}

#[test]
fn legacy_completed_copy_archive_is_read_without_rewriting_or_replay() {
    verify_copy_history(
        CopyProfile::LegacyCompleted,
        CopyInterruption::KillAcceptedPut,
    );
}

#[test]
fn legacy_interrupted_copy_archive_is_read_without_rewriting_or_replay() {
    verify_copy_history(
        CopyProfile::LegacyInterrupted,
        CopyInterruption::KillAcceptedPut,
    );
}

#[test]
fn local_to_s3_sigterm_after_accepted_put_preserves_unresolved_history() {
    verify_copy_history(
        CopyProfile::LocalSource,
        CopyInterruption::TerminateAcceptedPut,
    );
}

#[test]
fn s3_to_s3_sigterm_after_accepted_put_preserves_unresolved_history() {
    verify_copy_history(
        CopyProfile::S3Source,
        CopyInterruption::TerminateAcceptedPut,
    );
}

#[test]
fn local_to_s3_sigterm_after_saved_report_preserves_unresolved_history() {
    verify_copy_history(
        CopyProfile::LocalSource,
        CopyInterruption::TerminateSavedReport,
    );
}

#[test]
fn s3_to_s3_sigterm_after_saved_report_preserves_unresolved_history() {
    verify_copy_history(
        CopyProfile::S3Source,
        CopyInterruption::TerminateSavedReport,
    );
}

#[derive(Clone, Copy)]
enum CopyInterruption {
    KillAcceptedPut,
    TerminateAcceptedPut,
    TerminateSavedReport,
    TerminateSourceInventory,
}

#[test]
fn s3_to_s3_sigterm_during_source_inventory_preserves_checkpoint_without_writes() {
    verify_copy_history(
        CopyProfile::S3Source,
        CopyInterruption::TerminateSourceInventory,
    );
}

#[derive(Clone, Copy)]
enum CopyProfile {
    LocalSource,
    S3Source,
    ConfiguredDatabaseS3Source,
    UnsafeHistory,
    LegacyCompleted,
    LegacyInterrupted,
}

fn verify_copy_history(profile: CopyProfile, interruption: CopyInterruption) {
    let before_writes = matches!(interruption, CopyInterruption::TerminateSourceInventory);
    let local_source = matches!(profile, CopyProfile::LocalSource);
    let history_available = !matches!(profile, CopyProfile::UnsafeHistory);
    let legacy = matches!(
        profile,
        CopyProfile::LegacyCompleted | CopyProfile::LegacyInterrupted
    );
    let completed = matches!(profile, CopyProfile::LegacyCompleted);
    let unrelated_database = matches!(profile, CopyProfile::ConfiguredDatabaseS3Source);
    let binary = std::env::var_os("CARGO_BIN_EXE_shardline")
        .expect("set CARGO_BIN_EXE_shardline to the current optimized binary");
    let producer = if legacy {
        std::env::var_os("SHARDLINE_ARCHIVE_PREDECESSOR_BINARY")
            .expect("set SHARDLINE_ARCHIVE_PREDECESSOR_BINARY to the pre-project-history binary")
    } else {
        binary.clone()
    };
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
        if before_writes {
            S3ProxyFault::DelaySuccessfulGetBody(Duration::from_secs(3))
        } else {
            S3ProxyFault::DelaySuccessfulPutResponse(Duration::from_secs(3))
        },
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
    let mut command = Command::new(&producer);
    command
        .env_clear()
        .env("TMPDIR", scratch.path())
        .env("SHARDLINE_MIGRATE_TO_S3_BUCKET", &raw.bucket)
        .env("SHARDLINE_MIGRATE_TO_S3_REGION", &raw.region)
        .env(
            "SHARDLINE_MIGRATE_TO_S3_ENDPOINT",
            if before_writes {
                raw.endpoint.as_deref().unwrap()
            } else {
                proxy.endpoint()
            },
        )
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
                if before_writes {
                    proxy.endpoint()
                } else {
                    raw.endpoint.as_deref().unwrap()
                },
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
    if unrelated_database {
        command.env(
            "SHARDLINE_INDEX_POSTGRES_URL",
            "postgres://unused:unused@127.0.0.1:1/unused",
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
    if before_writes {
        assert!(remote.metadata(&key).unwrap().is_none());
        assert_checkpoint_saved(&scratch);
    } else {
        assert_eq!(
            remote
                .read_range(&key, shardline_protocol::ByteRange::new(0, end).unwrap())
                .unwrap(),
            bytes
        );
    }
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
    if completed {
        assert!(child.0.wait().unwrap().success());
    } else if matches!(
        interruption,
        CopyInterruption::TerminateAcceptedPut
            | CopyInterruption::TerminateSavedReport
            | CopyInterruption::TerminateSourceInventory
    ) {
        let _held_completion = if matches!(interruption, CopyInterruption::TerminateSavedReport) {
            Some(hold_completion_until_report(&scratch, &mut child))
        } else {
            assert!(
                !fs::read_to_string(scratch.path().join("cli.stdout"))
                    .unwrap()
                    .contains("inserted_objects:")
            );
            None
        };
        assert!(
            Command::new("kill")
                .arg("-TERM")
                .arg(child.0.id().to_string())
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(child.0.wait().unwrap().signal(), Some(15));
        if let Some(connection) = &_held_completion {
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM penelope_records_v1", [], |row| row
                        .get::<_, u32>(0))
                    .unwrap(),
                3
            );
        }
    } else {
        child.0.kill().unwrap();
        child.0.wait().unwrap();
    }
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
    if unrelated_database {
        history_command
            .env(
                "SHARDLINE_INDEX_POSTGRES_URL",
                "postgres://unused:unused@127.0.0.1:1/unused",
            )
            .arg("--local");
    }
    let archive_root = scratch.path().join(format!(
        "shardline-maintenance-{}-{}",
        fs::metadata(source.path()).unwrap().uid(),
        hex::encode(
            shardline_server::task_tracking::deployment_fingerprint(&source_config)
                .unwrap()
                .0
        ),
    ));
    let archive_before = if legacy {
        Some(archive_evidence(&archive_root))
    } else {
        None
    };
    if legacy {
        history_command
            .args(["--archive", "--root"])
            .arg(&archive_root);
    } else if local_source {
        history_command.arg("--root").arg(source.path());
    }
    let history = history_command.output().unwrap();
    if !local_source && !legacy {
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
    if unrelated_database {
        assert_eq!(
            page.get("history_source")
                .and_then(serde_json::Value::as_str),
            Some("local_sqlite")
        );
    }
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
        Some(if completed {
            "completed"
        } else {
            "needs_reconciliation"
        })
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
    if before_writes {
        assert!(remote.metadata(&key).unwrap().is_none());
        assert_checkpoint_saved(&scratch);
    } else {
        assert_eq!(
            remote
                .read_range(&key, shardline_protocol::ByteRange::new(0, end).unwrap())
                .unwrap(),
            bytes
        );
    }
    if let Some(before) = archive_before {
        if !completed {
            let workspace = before
                .keys()
                .find(|path| path.file_name().is_some_and(|name| name == "tasks.sqlite"))
                .and_then(|path| path.parent())
                .expect("predecessor must retain its native journal");
            let native = Command::new(&binary)
                .env_clear()
                .current_dir(source.path())
                .args(["history", "--archive", "--root"])
                .arg(workspace)
                .output()
                .unwrap();
            assert!(
                native.status.success(),
                "{}",
                String::from_utf8_lossy(&native.stderr)
            );
            let native_page: serde_json::Value = serde_json::from_slice(&native.stdout).unwrap();
            assert_eq!(
                native_page
                    .pointer("/tasks/0/state")
                    .and_then(serde_json::Value::as_str),
                Some("needs_reconciliation")
            );
            assert_eq!(
                native_page
                    .pointer("/tasks/0/owner/pid")
                    .and_then(serde_json::Value::as_u64),
                Some(u64::from(child.0.id()))
            );
            assert_eq!(
                native_page
                    .get("native_journal")
                    .and_then(serde_json::Value::as_bool),
                Some(true)
            );
        }
        for (path, bytes) in before {
            assert_eq!(
                fs::read(&path).unwrap(),
                bytes,
                "archive evidence changed: {}",
                path.display()
            );
        }
        assert_eq!(
            page.get("archive").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(
            page.get("history_notice")
                .and_then(serde_json::Value::as_str)
                .unwrap()
                .contains("without a deployment filter")
        );
    }
}

fn native_workspace(scratch: &TempDir) -> std::path::PathBuf {
    let state = fs::read_dir(scratch.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.is_dir()
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy().starts_with("shardline-maintenance-")
                })
        })
        .expect("copy must retain its native state inside the owned TMPDIR");
    fs::read_dir(&state)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.join("tasks.sqlite").is_file())
        .expect("copy must retain its admitted native workspace")
}

fn assert_checkpoint_saved(scratch: &TempDir) {
    let connection = rusqlite::Connection::open_with_flags(
        native_workspace(scratch).join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM penelope_records_v1", [], |row| row
                .get::<_, u32>(0))
            .unwrap(),
        3
    );
}

fn hold_completion_until_report(scratch: &TempDir, child: &mut StopOnDrop) -> rusqlite::Connection {
    let workspace = native_workspace(scratch);
    let connection = rusqlite::Connection::open_with_flags(
        workspace.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .unwrap();
    connection.busy_timeout(Duration::from_millis(100)).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM penelope_records_v1", [], |row| row
                .get::<_, u32>(0))
            .unwrap(),
        3,
        "report cut must precede the completion acknowledgement"
    );
    let started = Instant::now();
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "remote copy did not materialize its result"
        );
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "copy exited before its report cut"
        );
        let saved_report = fs::read_dir(workspace.join("artifacts"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
            })
            .any(|path| {
                fs::read(path)
                    .unwrap()
                    .starts_with(b"maintenance.success: true\n")
            });
        if saved_report {
            assert!(
                fs::read_to_string(scratch.path().join("cli.stdout"))
                    .unwrap()
                    .contains("inserted_objects: 1")
            );
            return connection;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn archive_evidence(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut evidence = std::collections::BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            evidence.extend(archive_evidence(&path));
        } else if entry.file_type().unwrap().is_file()
            && !entry.file_name().to_string_lossy().ends_with("-shm")
        {
            // SQLite SHM contains read-lock bookkeeping; database, WAL,
            // journals, identities, signing keys and artifacts are evidence.
            evidence.insert(path.clone(), fs::read(path).unwrap());
        }
    }
    evidence
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
