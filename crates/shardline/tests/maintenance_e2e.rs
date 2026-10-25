#![cfg(target_os = "linux")]
#![allow(clippy::panic_in_result_fn)]
use penelope::core::task::Digest;
use rusqlite::Connection;
use shardline_reliability::MaintenanceArtifactProof;
use std::{
    fs::{self, OpenOptions},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
mod maintenance_support;

fn command(state: &Path, root: &Path, operation: &str) -> Command {
    let mut command = Command::new(binary());
    command
        .env_clear()
        .current_dir(root)
        .args(["maintenance", "--state-dir"])
        .arg(state)
        .args(["--task-id", "test-task", operation, "--root"])
        .arg(root);
    command
}

fn run_operation(state: &Path, root: &Path, operation: &str) -> Result<std::process::Output> {
    run_cli(&mut command(state, root, operation))
}

fn run_cli(command: &mut Command) -> Result<std::process::Output> {
    Ok(shardline_test_support::native_client_output(
        command,
        Duration::from_secs(120),
    )?)
}

fn journal(state: &Path) -> std::path::PathBuf {
    state
        .join(hex::encode(Digest::of(b"test-task").0))
        .join("tasks.sqlite")
}

fn binary() -> std::ffi::OsString {
    std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into())
}

fn maintenance_identities(
    root: &Path,
) -> Result<Vec<shardline_server::task_tracking::TaskRunIdentity>> {
    let connection = Connection::open_with_flags(
        root.join("metadata.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut query = connection.prepare("SELECT payload FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*' ORDER BY artifact_key")?;
    let rows = query.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| Ok(serde_json::from_slice(&row?)?)).collect()
}

fn assert_private_scratch_cleaned(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root.join(".shardline-maintenance"))? {
        let entry = entry?;
        assert_eq!(
            entry.file_name(),
            "retired",
            "completed scratch must not remain"
        );
        assert!(entry.file_type()?.is_dir());
        assert_eq!(
            fs::read_dir(entry.path())?.count(),
            0,
            "retired scratch must be removed"
        );
    }
    Ok(())
}

#[test]
fn ordinary_maintenance_preserves_existing_writable_files_under_read_only_root() -> Result<()> {
    use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunState};
    use std::os::unix::fs::PermissionsExt;
    if rustix::process::geteuid().as_raw() == 0 {
        eprintln!("skipping permission regression: root bypasses directory permissions");
        return Ok(());
    }
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    let bootstrap = Command::new(binary())
        .env_clear()
        .current_dir(sandbox.path())
        .args(["index", "rebuild", "--root"])
        .arg(&root)
        .output()?;
    assert!(bootstrap.status.success(), "{bootstrap:?}");
    fs::remove_dir_all(root.join(".shardline-maintenance"))?;
    // A running deployment keeps its SQLite sidecars open. The database and
    // sidecars remain writable even when operators disallow new root entries.
    let connection = Connection::open(root.join("metadata.sqlite3"))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; SELECT COUNT(*) FROM sqlite_master;")?;
    assert!(root.join("metadata.sqlite3-wal").is_file());
    assert!(root.join("metadata.sqlite3-shm").is_file());
    let config = shardline_server::ServerConfig::new(
        "127.0.0.1:8080".parse()?,
        "http://localhost:8080".to_owned(),
        root.clone(),
        std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
    );
    let store = TaskEvidenceStore::from_config(&config)?;
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
    let previous = store.summaries(deployment, None, 20)?.len();
    struct RestorePermissions(std::path::PathBuf);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            let _ignored = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
        }
    }
    let _restore = RestorePermissions(root.clone());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o555))?;
    assert_eq!(
        fs::create_dir(root.join("permission-probe"))
            .err()
            .ok_or("fixture must deny new root entries")?
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    for arguments in [
        vec!["fsck"],
        vec!["index", "rebuild"],
        vec!["repair", "lifecycle"],
        vec!["gc", "--mark"],
    ] {
        let output = Command::new(binary())
            .env_clear()
            .current_dir(sandbox.path())
            .args(&arguments)
            .arg("--root")
            .arg(&root)
            .output()?;
        assert!(output.status.success(), "{arguments:?}: {output:?}");
        assert!(output.stderr.is_empty(), "{arguments:?}: {output:?}");
        assert!(!root.join(".shardline-maintenance").exists());
        assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o777, 0o555);
    }
    let summaries = store.summaries(deployment, None, 20)?;
    assert_eq!(summaries.len(), previous + 4);
    assert!(
        summaries
            .iter()
            .all(|run| run.state == TaskRunState::Completed)
    );
    Ok(())
}

fn admitted_inline_directory(state: &Path) -> Option<std::path::PathBuf> {
    for entry in fs::read_dir(state).ok()?.flatten() {
        let path = entry.path().join("tasks.sqlite");
        let Ok(connection) =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        else {
            continue;
        };
        if connection
            .query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get::<_, u32>(0)
            })
            .ok()
            == Some(3)
        {
            return Some(entry.path());
        }
    }
    None
}

#[test]
fn first_maintenance_initializes_tracking_without_narrowing_deployment_permissions() -> Result<()> {
    use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunState};
    use std::os::unix::fs::PermissionsExt;
    for arguments in [
        vec!["fsck"],
        vec!["index", "rebuild"],
        vec!["gc", "--mark"],
        vec!["repair", "lifecycle"],
    ] {
        let sandbox = tempfile::tempdir()?;
        let reference = sandbox.path().join("reference/nested/deployment");
        fs::create_dir_all(&reference)?;
        let expected_mode = fs::metadata(&reference)?.permissions().mode() & 0o777;
        let root = sandbox.path().join("actual/nested/deployment");
        let output = Command::new(binary())
            .env_clear()
            .current_dir(sandbox.path())
            .args(arguments)
            .arg("--root")
            .arg(&root)
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        for directory in [
            root.clone(),
            root.parent().ok_or("parent")?.to_path_buf(),
            root.parent()
                .and_then(Path::parent)
                .ok_or("ancestor")?
                .to_path_buf(),
        ] {
            assert_eq!(
                fs::metadata(directory)?.permissions().mode() & 0o777,
                expected_mode
            );
        }
        assert_eq!(
            fs::metadata(root.join(".shardline-maintenance"))?
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let config = shardline_server::ServerConfig::new(
            "127.0.0.1:8080".parse()?,
            "http://localhost:8080".to_owned(),
            root,
            std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
        );
        let store = TaskEvidenceStore::from_config(&config)?;
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
        let tasks = store.summaries(deployment, None, 100)?;
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks.first().ok_or("missing task")?.state,
            TaskRunState::Completed
        );
    }
    Ok(())
}

#[test]
fn invalid_root_preserves_original_validation_status_without_duplicate_tracking_warning()
-> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("not-a-directory");
    fs::write(&root, b"existing deployment diagnostic fixture")?;
    let output = Command::new(binary())
        .env_clear()
        .current_dir(sandbox.path())
        .args(["fsck", "--root"])
        .arg(&root)
        .output()?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("maintenance tracking could not initialize"));
    assert!(
        !stderr.trim().is_empty(),
        "the original diagnostic must remain visible"
    );
    assert_eq!(fs::read(&root)?, b"existing deployment diagnostic fixture");
    assert!(!sandbox.path().join(".shardline-maintenance").exists());
    Ok(())
}

fn wait_for(mut check: impl FnMut() -> bool) -> Result<()> {
    let start = Instant::now();
    while !check() {
        if start.elapsed() > Duration::from_secs(180) {
            return Err("worker test timed out".into());
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

#[test]
fn normal_commands_allocate_fresh_runs_and_publish_evidence_without_setup() -> Result<()> {
    use penelope::core::task::{TaskProjection, TaskRecord, TaskStatus};
    use shardline_server::task_tracking::TaskEvidenceStore;
    for arguments in [
        vec!["fsck"],
        vec!["index", "rebuild"],
        vec!["gc", "--mark"],
        vec!["repair", "lifecycle"],
    ] {
        let sandbox = tempfile::tempdir()?;
        let root = sandbox.path().join("deployment");
        fs::create_dir(&root)?;
        let mut results = Vec::new();
        for _ in 0..2 {
            let output = Command::new(binary())
                .env_clear()
                .current_dir(&root)
                .args(&arguments)
                .arg("--root")
                .arg(&root)
                .output()?;
            assert!(
                output.status.success(),
                "{arguments:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("maintenance.task:"));
            results.push(output);
        }
        let manifests = maintenance_identities(&root)?;
        assert_eq!(
            manifests.len(),
            2,
            "each intentional invocation must perform a fresh run"
        );
        let config = shardline_server::ServerConfig::new(
            "127.0.0.1:8080".parse()?,
            "http://localhost:8080".to_owned(),
            root.clone(),
            std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
        );
        let store = TaskEvidenceStore::from_config(&config)?;
        assert!(!root.join("chunks/_shardline_tasks/v1").exists());
        for identity in manifests {
            assert_eq!(identity.definition.budget.cpu_ticks, u64::MAX);
            assert_eq!(identity.definition.budget.deadline_tick.get(), u64::MAX);
            let mut projection = TaskProjection::new(identity.scope, identity.definition)?;
            loop {
                let suffix = format!(
                    "runs/{}/records/{:010}",
                    identity.id,
                    projection.next_sequence().get()
                );
                let Some(bytes) = store.read(&suffix, 16384)? else {
                    break;
                };
                let record: TaskRecord = serde_json::from_slice(&bytes)?;
                projection.apply(&record)?;
            }
            assert_eq!(projection.status(), TaskStatus::Completed);
            let output = projection.output().ok_or("missing result")?;
            let proof_bytes = store
                .read(
                    &format!(
                        "runs/{}/blobs/{}",
                        identity.id,
                        hex::encode(output.digest.0)
                    ),
                    65536,
                )?
                .ok_or("missing proof")?;
            let proof = MaintenanceArtifactProof::from_json(&proof_bytes)?;
            let public_key = ed25519_dalek::VerifyingKey::from_bytes(&identity.public_key)?;
            shardline_reliability::verify_maintenance_artifact(
                &proof,
                identity.scope,
                projection.generation(),
                &public_key,
                &statechronicle::domain::intent::KeyId(format!(
                    "shardline-maintenance:{}",
                    hex::encode(identity.public_key)
                )),
            )?;
            let body = store
                .read(
                    &format!(
                        "runs/{}/artifacts/{}",
                        identity.id,
                        hex::encode(proof.artifact.digest.0)
                    ),
                    65536,
                )?
                .ok_or("missing report manifest")?;
            proof.artifact.verify_materialized(body.as_slice())?;
            assert_eq!(
                identity.execution,
                shardline_server::task_tracking::TaskExecutionMode::Inline
            );
            let outcome: serde_json::Value = serde_json::from_slice(
                body.split(|byte| *byte == b'\n')
                    .nth(1)
                    .ok_or("missing outcome")?,
            )?;
            assert_eq!(outcome.get("exit_code"), Some(&serde_json::json!(0)));
            assert!(
                results.iter().all(|result| !result.stdout.is_empty()),
                "original handlers must retain their output"
            );
        }
        assert_private_scratch_cleaned(&root)?;
    }
    Ok(())
}

#[test]
fn shared_publication_failure_preserves_existing_command_and_local_supervision() -> Result<()> {
    for previous_boot in [false, true] {
        if previous_boot
            && shardline_server::task_tracking::TaskOwner::current()
                .host
                .is_none()
        {
            // A host without stable identity cannot prove a same-host reboot.
            continue;
        }
        let sandbox = tempfile::tempdir()?;
        let root = sandbox.path().join("deployment");
        fs::create_dir_all(&root)?;
        let config = shardline::load_server_config(Some(&root), None)?;
        let store = shardline_server::task_tracking::TaskEvidenceStore::from_config(&config)?;
        store.publish("runs/fixture/blobs/seed", b"publication fixture")?;
        let metadata = Connection::open(root.join("metadata.sqlite3"))?;
        metadata.execute_batch(
            "CREATE TRIGGER deny_task_publication BEFORE INSERT ON shardline_task_artifacts_v1
             BEGIN SELECT RAISE(ABORT, 'fixture denies shared publication'); END;",
        )?;
        let output = Command::new(binary())
            .env_clear()
            .current_dir(&root)
            .args(["index", "rebuild", "--root"])
            .arg(&root)
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            root.join("metadata.sqlite3").exists(),
            "shared audit permissions must not break existing maintenance"
        );
        assert_eq!(maintenance_identities(&root)?.len(), 0);
        assert_eq!(
            fs::read_dir(root.join(".shardline-maintenance"))?.count(),
            1
        );
        for entry in fs::read_dir(root.join(".shardline-maintenance"))? {
            let directory = entry?.path();
            let connection = Connection::open_with_flags(
                directory.join("tasks.sqlite"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            assert_eq!(
                connection.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                    row.get::<_, u32>(0)
                })?,
                4
            );
            if previous_boot {
                let path = directory.join("identity.json");
                let mut identity: shardline_server::task_tracking::TaskRunIdentity =
                    serde_json::from_slice(&fs::read(&path)?)?;
                identity.owner.boot = Some("previous-boot".to_owned());
                identity.owner.namespace = Some(
                    identity
                        .owner
                        .namespace
                        .unwrap_or_default()
                        .saturating_add(1),
                );
                fs::write(path, serde_json::to_vec(&identity)?)?;
            }
        }
        metadata.execute_batch("DROP TRIGGER deny_task_publication")?;
        let subsequent = Command::new(binary())
            .env_clear()
            .current_dir(&root)
            .args(["index", "rebuild", "--root"])
            .arg(&root)
            .output()?;
        assert!(
            subsequent.status.success(),
            "{}",
            String::from_utf8_lossy(&subsequent.stderr)
        );
        assert_eq!(maintenance_identities(&root)?.len(), 2);
        assert_private_scratch_cleaned(&root)?;
        assert_eq!(
            store.read("runs/fixture/blobs/seed", 65536)?.as_deref(),
            Some(b"publication fixture".as_slice())
        );
        assert!(!root.join("chunks/_shardline_tasks/v1").exists());
    }
    Ok(())
}

#[test]
fn ordinary_gc_dry_run_does_not_require_or_write_deployment_audit_storage() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("chunks/_shardline_tasks/v1"))?;
    fs::write(
        root.path().join("chunks/_shardline_tasks/v1/manifests"),
        b"unwritable audit namespace",
    )?;
    let output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["gc", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("mode: dry-run"));
    assert!(!root.path().join(".shardline-maintenance").exists());
    assert_eq!(
        fs::read(root.path().join("chunks/_shardline_tasks/v1/manifests"))?,
        b"unwritable audit namespace"
    );
    let fresh = root.path().join("fresh/nested/deployment");
    let fresh_output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["gc", "--root"])
        .arg(&fresh)
        .output()?;
    assert!(
        fresh_output.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh_output.stderr)
    );
    assert!(
        fresh_output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&fresh_output.stderr)
    );
    assert!(String::from_utf8_lossy(&fresh_output.stdout).contains("mode: dry-run"));
    assert!(fresh.is_dir());
    assert!(!fresh.join(".shardline-maintenance").exists());
    assert!(!fresh.join("chunks/_shardline_tasks").exists());
    Ok(())
}

#[test]
fn ordinary_commands_preserve_non_utf8_paths() -> Result<()> {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let sandbox = tempfile::tempdir()?;
    let root = sandbox
        .path()
        .join(OsString::from_vec(b"deployment-\xff".to_vec()));
    fs::create_dir(&root)?;
    let output = Command::new(binary())
        .env_clear()
        .current_dir(&root)
        .args(["index", "rebuild", "--root"])
        .arg(&root)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.join("metadata.sqlite3").exists());
    assert_eq!(maintenance_identities(&root)?.len(), 1);
    Ok(())
}

#[test]
fn killed_ordinary_maintenance_remains_unresolved_and_is_not_replayed() -> Result<()> {
    use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunIdentity, TaskRunState};
    struct StopOnDrop(std::process::Child);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            let _kill = self.0.kill();
            let _wait = self.0.wait();
        }
    }
    let root = tempfile::tempdir()?;
    let barrier = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.path().join(".gc-write-barrier.lock"))?;
    barrier.lock()?;
    let child = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut child = StopOnDrop(child);
    let state = root.path().join(".shardline-maintenance");
    let mut interrupted = None;
    wait_for(|| {
        interrupted = admitted_inline_directory(&state);
        interrupted.is_some()
    })?;
    let directory = interrupted.ok_or("no admitted run")?;
    let identity: TaskRunIdentity =
        serde_json::from_slice(&fs::read(directory.join("identity.json"))?)?;
    assert_eq!(identity.owner.pid, child.0.id());
    child.0.kill()?;
    let _status = child.0.wait()?;
    let config = shardline_server::ServerConfig::new(
        "127.0.0.1:8080".parse()?,
        "http://localhost:8080".to_owned(),
        root.path().to_path_buf(),
        std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
    );
    let store = TaskEvidenceStore::from_config(&config)?;
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
    assert!(
        store
            .summaries(deployment, None, 100)?
            .iter()
            .any(|task| task.id == identity.id && task.state == TaskRunState::NeedsReconciliation)
    );
    barrier.unlock()?;
    // An intentional integrity scan may run, but exporting old evidence must
    // neither retry the interrupted rebuild nor append a success to its journal.
    let output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["fsck", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let connection = Connection::open_with_flags(
        directory.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    assert_eq!(
        connection.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        3
    );
    let summaries = store.summaries(deployment, None, 100)?;
    assert_eq!(summaries.len(), 2);
    assert!(
        summaries
            .iter()
            .any(|task| task.id == identity.id && task.state == TaskRunState::NeedsReconciliation)
    );
    assert!(
        summaries
            .iter()
            .any(|task| task.id != identity.id && task.state == TaskRunState::Completed)
    );
    Ok(())
}

#[test]
fn ordinary_maintenance_uses_the_original_cli_process() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut child = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let pid = child.id();
    while child.try_wait()?.is_none() {
        if let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) {
            for task in tasks {
                if let Ok(children) = fs::read_to_string(task?.path().join("children")) {
                    assert!(
                        children.trim().is_empty(),
                        "ordinary maintenance must not fork another worker: {children}"
                    );
                }
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifests = maintenance_identities(root.path())?;
    assert_eq!(manifests.len(), 1);
    let identity = manifests.first().ok_or("missing run")?;
    assert_eq!(identity.owner.pid, pid);
    assert_eq!(
        identity.execution,
        shardline_server::task_tracking::TaskExecutionMode::Inline
    );
    Ok(())
}

#[test]
fn ordinary_commands_relocate_finished_legacy_history_incrementally() -> Result<()> {
    use shardline_server::task_tracking::{TaskEvidenceStore, deployment_fingerprint};
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
    let root = tempfile::tempdir()?;
    let config = shardline::load_server_config(Some(root.path()), None)?;
    let deployment = deployment_fingerprint(&config)?;
    let objects = shardline_server::object_store_from_config(&config)?;
    let seed = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .output()?;
    assert!(seed.status.success());
    let legacy = TaskEvidenceStore::from_config(&config)?;
    let id = legacy
        .summaries(deployment, None, 10)?
        .into_iter()
        .next()
        .ok_or("missing legacy run")?
        .id;
    let publish_legacy = |suffix: &str, bytes: &[u8]| -> Result<()> {
        let key = ObjectKey::parse(&format!("_shardline_tasks/v1/{suffix}"))?;
        let hash = shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes());
        objects.put_if_absent(
            &key,
            ObjectBody::Borrowed(bytes),
            &ObjectIntegrity::new(hash, bytes.len().try_into()?),
        )?;
        Ok(())
    };
    let database = Connection::open(root.path().join("metadata.sqlite3"))?;
    {
        let mut statement =
            database.prepare("SELECT artifact_key, payload FROM shardline_task_artifacts_v1")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        for row in rows {
            let (key, bytes) = row?;
            publish_legacy(
                key.strip_prefix("_shardline_tasks/v1/")
                    .ok_or("unexpected history key")?,
                &bytes,
            )?;
        }
    }
    database.execute("DELETE FROM shardline_task_artifacts_v1", [])?;
    drop(database);
    for number in 0..80 {
        publish_legacy(
            &format!("runs/{id}/extra/{number:04}"),
            b"original legacy evidence",
        )?;
    }
    publish_legacy(
        "manifests/unrecognized-fixture",
        b"preserve unrecognized evidence",
    )?;
    let connection = || {
        Connection::open_with_flags(
            root.path().join("metadata.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
    };
    let payload = root.path().join("chunks/user-payload");
    fs::write(&payload, b"preserved payload bytes")?;
    let prefix =
        shardline_storage::ObjectPrefix::parse(&format!("_shardline_tasks/v1/runs/{id}/"))?;
    let before = objects.list_prefix(&prefix)?.len();
    for invocation in 0..2 {
        let output = Command::new(binary())
            .env_clear()
            .current_dir(root.path())
            .args(["index", "rebuild", "--root"])
            .arg(root.path())
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("scanned_version_records: 0"));
        assert_eq!(fs::read(&payload)?, b"preserved payload bytes");
        assert_eq!(
            legacy
                .read("manifests/unrecognized-fixture", 65536)?
                .as_deref(),
            Some(b"preserve unrecognized evidence".as_slice())
        );
        let remaining = objects.list_prefix(&prefix)?.len();
        let published: u32 = connection()?.query_row(
            "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key=?1",
            [format!("_shardline_tasks/v1/manifests/{id}")],
            |row| row.get(0),
        )?;
        if invocation == 0 {
            assert_eq!(remaining, before - 64);
            assert_eq!(published, 0, "the legacy manifest must move last");
            assert!(legacy.read(&format!("manifests/{id}"), 65536)?.is_some());
        } else {
            assert_eq!(remaining, 0);
            assert_eq!(published, 1);
            assert!(
                objects
                    .list_prefix(&shardline_storage::ObjectPrefix::parse(&format!(
                        "_shardline_tasks/v1/manifests/{id}"
                    ))?)?
                    .is_empty()
            );
        }
    }
    let store = TaskEvidenceStore::from_config(&config)?;
    for number in 0..80 {
        assert_eq!(
            store
                .read(&format!("runs/{id}/extra/{number:04}"), 65536)?
                .as_deref(),
            Some(b"original legacy evidence".as_slice())
        );
    }
    assert_private_scratch_cleaned(root.path())?;
    Ok(())
}

#[test]
fn ordinary_storage_migration_tracks_at_destination_and_preserves_dry_run_counters() -> Result<()> {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let objects = shardline_server::ServerObjectStore::local(source.path().join("chunks"))?;
    let bytes = b"immutable migration bytes";
    let key = ObjectKey::parse("test/object")?;
    let hash = shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes());
    objects.put_if_absent(
        &key,
        ObjectBody::Borrowed(bytes),
        &ObjectIntegrity::new(hash, bytes.len().try_into()?),
    )?;
    let invoke = |dry_run: bool| -> Result<std::process::Output> {
        let mut command = Command::new(binary());
        command
            .env_clear()
            .current_dir(source.path())
            .args(["storage", "migrate", "--from", "local", "--from-root"])
            .arg(source.path())
            .args(["--to", "local", "--to-root"])
            .arg(destination.path());
        if dry_run {
            command.arg("--dry-run");
        }
        run_cli(&mut command)
    };
    let dry = invoke(true)?;
    assert!(
        dry.status.success(),
        "migration dry run failed: {}",
        dry.status
    );
    assert!(!destination.path().join("chunks/test/object").exists());
    assert!(!source.path().join("chunks/_shardline_tasks/v1").exists());
    assert!(
        !destination
            .path()
            .join("chunks/_shardline_tasks/v1")
            .exists()
    );
    let real = invoke(false)?;
    assert!(real.status.success(), "migration failed: {}", real.status);
    assert_eq!(
        fs::read(destination.path().join("chunks/test/object"))?,
        bytes
    );
    assert!(!source.path().join("chunks/_shardline_tasks/v1").exists());
    assert_eq!(maintenance_identities(destination.path())?.len(), 1);
    let repeated = invoke(false)?;
    assert!(
        repeated.status.success(),
        "repeated migration failed: {}",
        repeated.status
    );
    for output in [dry, real, repeated] {
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("scanned_objects: 1"),
            "migration did not report exactly one scanned object"
        );
    }
    Ok(())
}

#[test]
fn storage_copy_preserves_success_when_destination_history_publication_fails() -> Result<()> {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let objects = shardline_server::ServerObjectStore::local(source.path().join("chunks"))?;
    let bytes = b"copy must survive optional history failure";
    let key = ObjectKey::parse("test/object")?;
    let hash = shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes());
    objects.put_if_absent(
        &key,
        ObjectBody::Borrowed(bytes),
        &ObjectIntegrity::new(hash, bytes.len().try_into()?),
    )?;
    let config = shardline::load_server_config(Some(destination.path()), None)?;
    let store = shardline_server::task_tracking::TaskEvidenceStore::from_config(&config)?;
    store.publish("runs/fixture/blobs/seed", b"existing destination evidence")?;
    let metadata = Connection::open(destination.path().join("metadata.sqlite3"))?;
    metadata.execute_batch(
        "CREATE TRIGGER deny_task_publication BEFORE INSERT ON shardline_task_artifacts_v1
         BEGIN SELECT RAISE(ABORT, 'fixture denies destination history'); END;",
    )?;
    let invoke = || -> Result<std::process::Output> {
        run_cli(
            Command::new(binary())
                .env_clear()
                .current_dir(source.path())
                .args(["storage", "migrate", "--from", "local", "--from-root"])
                .arg(source.path())
                .args(["--to", "local", "--to-root"])
                .arg(destination.path()),
        )
    };
    let output = invoke()?;
    assert!(
        output.status.success(),
        "storage copy failed: {}",
        output.status
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("scanned_objects: 1"));
    assert_eq!(fs::read(source.path().join("chunks/test/object"))?, bytes);
    assert_eq!(
        fs::read(destination.path().join("chunks/test/object"))?,
        bytes
    );
    assert!(maintenance_identities(destination.path())?.is_empty());
    assert_eq!(
        store.read("runs/fixture/blobs/seed", 100)?,
        Some(b"existing destination evidence".to_vec())
    );
    metadata.execute_batch("DROP TRIGGER deny_task_publication")?;
    let next = invoke()?;
    assert!(
        next.status.success(),
        "storage copy retry failed: {}",
        next.status
    );
    assert_eq!(
        fs::read(destination.path().join("chunks/test/object"))?,
        bytes
    );
    assert!(!maintenance_identities(destination.path())?.is_empty());
    assert_eq!(
        store.read("runs/fixture/blobs/seed", 100)?,
        Some(b"existing destination evidence".to_vec())
    );
    Ok(())
}

#[test]
fn real_workflows_commit_native_checkpoints_and_reuse_completed_history() -> Result<()> {
    for operation in ["fsck", "index-rebuild", "gc", "lifecycle-repair"] {
        let sandbox = tempfile::tempdir()?;
        let root = sandbox.path().join("deployment");
        fs::create_dir(&root)?;
        let state = sandbox.path().join("maintenance");
        let first = run_operation(&state, &root, operation)?;
        assert!(first.status.success(), "{operation}: {}", first.status);
        let connection = Connection::open(journal(&state))?;
        let records: u32 =
            connection.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get(0)
            })?;
        let repeated = run_operation(&state, &root, operation)?;
        assert!(
            repeated.status.success(),
            "repeat {operation}: {}",
            repeated.status
        );
        assert_eq!(
            connection.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
                .get::<_, u32>(0))?,
            records
        );
        let directory = journal(&state)
            .parent()
            .ok_or("state directory")?
            .to_path_buf();
        let proof =
            MaintenanceArtifactProof::from_json(&fs::read(directory.join("result-proof.json"))?)?;
        assert_eq!(proof.artifact.step, 2);
        assert_eq!(proof.artifact.generation.get(), 1);
        proof.artifact.verify_materialized(fs::File::open(
            directory
                .join("artifacts")
                .join(hex::encode(proof.artifact.digest.0)),
        )?)?;
        let different = command(
            &state,
            &root,
            if operation == "fsck" {
                "index-rebuild"
            } else {
                "fsck"
            },
        )
        .output()?;
        assert!(!different.status.success());
        assert!(String::from_utf8_lossy(&different.stderr).contains("different request"));
    }
    Ok(())
}

#[test]
fn completed_task_rejects_materialized_report_tampering() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    fs::create_dir(&root)?;
    let state = sandbox.path().join("maintenance");
    assert!(run_operation(&state, &root, "fsck")?.status.success());
    let directory = journal(&state)
        .parent()
        .ok_or("state directory")?
        .to_path_buf();
    let proof =
        MaintenanceArtifactProof::from_json(&fs::read(directory.join("result-proof.json"))?)?;
    fs::write(
        directory
            .join("artifacts")
            .join(hex::encode(proof.artifact.digest.0)),
        b"altered",
    )?;
    assert!(!run_operation(&state, &root, "fsck")?.status.success());
    Ok(())
}

#[test]
fn interrupted_mutation_is_fenced_and_remains_unknown() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    fs::create_dir(&root)?;
    let barrier = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".gc-write-barrier.lock"))?;
    barrier.lock()?;
    let state = sandbox.path().join("maintenance");
    let mut controller = command(&state, &root, "index-rebuild")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_for(|| {
        Connection::open_with_flags(journal(&state), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok().and_then(|connection|
            connection.query_row("SELECT EXISTS(SELECT 1 FROM penelope_runtime_v1 WHERE pid IS NOT NULL AND acknowledged IS NOT NULL)", [], |row| row.get::<_, bool>(0)).ok()).unwrap_or(false)
    })?;
    let worker_pid: u32 =
        Connection::open_with_flags(journal(&state), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?
            .query_row("SELECT pid FROM penelope_runtime_v1", [], |row| row.get(0))?;
    let concurrent = run_operation(&state, &root, "index-rebuild")?;
    assert!(!concurrent.status.success());
    controller.kill()?;
    let _status = controller.wait()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let worker_stopped = loop {
        let stopped = match fs::read_to_string(format!("/proc/{worker_pid}/stat")) {
            Ok(stat) => {
                stat.rsplit_once(')')
                    .and_then(|(_, fields)| fields.split_whitespace().next())
                    == Some("Z")
            }
            Err(error) => error.kind() == std::io::ErrorKind::NotFound,
        };
        if stopped || Instant::now() >= deadline {
            break stopped;
        }
        thread::sleep(Duration::from_millis(5));
    };
    // Recovery cleans up a surviving fixture child even if this assertion fails.
    let recovery = run_operation(&state, &root, "index-rebuild")?;
    assert!(
        worker_stopped,
        "optional worker survived controller termination"
    );
    assert!(!recovery.status.success());
    assert!(
        String::from_utf8_lossy(&recovery.stderr).contains("effects remain unknown"),
        "{}",
        String::from_utf8_lossy(&recovery.stderr)
    );
    let connection = Connection::open(journal(&state))?;
    assert!(
        connection.query_row("SELECT fenced FROM penelope_runtime_v1", [], |row| row
            .get::<_, bool>(0))?
    );
    assert_eq!(
        connection.query_row("SELECT count(*) FROM penelope_runtime_v1", [], |row| row
            .get::<_, u32>(0))?,
        1
    );
    barrier.unlock()?;
    Ok(())
}

#[test]
fn state_inside_deployment_and_untrusted_worker_invocation_are_rejected() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    fs::create_dir(&root)?;
    assert!(
        !run_operation(&root.join("state"), &root, "fsck")?
            .status
            .success()
    );
    assert!(!root.join("state").exists());
    let alias = sandbox.path().join("alias");
    fs::create_dir(&alias)?;
    assert!(
        !run_operation(&alias.join("../deployment/state"), &root, "fsck")?
            .status
            .success()
    );
    assert!(!root.join("state").exists());
    let worker = run_cli(
        Command::new(binary())
            .args(["maintenance-worker", "--journal"])
            .arg(root.join("missing.sqlite"))
            .args(["--scope", "00", "--generation", "1"]),
    )?;
    assert!(!worker.status.success());
    assert!(!root.join("missing.sqlite").exists());
    Ok(())
}

#[test]
fn explicit_configuration_reaches_workers_and_changed_targets_cannot_resume() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    fs::create_dir(&root)?;
    let state = sandbox.path().join("maintenance");
    let config = sandbox.path().join("active config.toml");
    fs::write(&config, "[server]\nfrontends = [\"s3\"]\n")?;
    let output = run_cli(command(&state, &root, "fsck").arg("--config").arg(&config))?;
    assert!(
        output.status.success(),
        "configured maintenance failed: {}",
        output.status
    );
    fs::write(&config, "[server]\nfrontends = [\"xet\"]\n")?;
    let changed = run_cli(command(&state, &root, "fsck").arg("--config").arg(&config))?;
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("different request"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_copies_real_objects_and_pins_the_destination() -> Result<()> {
    use bytes::Bytes;
    use shardline_server::LocalBackend;
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("source");
    let destination = sandbox.path().join("destination");
    fs::create_dir(&root)?;
    fs::create_dir(&destination)?;
    let backend = LocalBackend::new(
        root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        std::num::NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    let _uploaded = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"native-maintenance-copy"),
            None,
        )
        .await?;
    let state = sandbox.path().join("maintenance");
    let first = run_cli(
        command(&state, &root, "storage-migrate")
            .args(["--from", "local", "--to", "local", "--to-root"])
            .arg(&destination),
    )?;
    assert!(
        first.status.success(),
        "supervised storage migration failed: {}",
        first.status
    );
    let directory = journal(&state)
        .parent()
        .ok_or("state directory")?
        .to_path_buf();
    let proof =
        MaintenanceArtifactProof::from_json(&fs::read(directory.join("result-proof.json"))?)?;
    let report = fs::read_to_string(
        directory
            .join("artifacts")
            .join(hex::encode(proof.artifact.digest.0)),
    )?;
    assert!(
        !report.contains("inserted_objects: 0"),
        "migration must insert objects"
    );
    assert!(destination.join("chunks").is_dir());
    let other = sandbox.path().join("other");
    fs::create_dir(&other)?;
    assert!(
        !run_cli(
            command(&state, &root, "storage-migrate")
                .args(["--from", "local", "--to", "local", "--to-root"])
                .arg(&other)
        )?
        .status
        .success()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_completed_scan_with_missing_objects_preserves_failure_exit_status() -> Result<()> {
    use bytes::Bytes;
    use shardline_server::LocalBackend;
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    fs::create_dir(&root)?;
    let backend = LocalBackend::new(
        root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        std::num::NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    let _uploaded = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"missing-native-maintenance-object"),
            None,
        )
        .await?;
    fs::remove_dir_all(root.join("chunks"))?;
    fs::create_dir(root.join("chunks"))?;
    let ordinary = shardline_test_support::native_client_output(
        Command::new(binary())
            .env_clear()
            .current_dir(&root)
            .args(["fsck", "--root"])
            .arg(&root),
        Duration::from_secs(60),
    )?;
    assert_eq!(ordinary.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&ordinary.stdout).contains("maintenance.task:"));
    let config = shardline_server::ServerConfig::new(
        "127.0.0.1:8080".parse()?,
        "http://localhost:8080".to_owned(),
        root.clone(),
        std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
    );
    let store = shardline_server::task_tracking::TaskEvidenceStore::from_config(&config)?;
    let summaries = store.summaries(
        shardline_server::task_tracking::deployment_fingerprint(&config)?,
        None,
        10,
    )?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        summaries.first().ok_or("missing outcome")?.state,
        shardline_server::task_tracking::TaskRunState::Failed
    );
    // Ordinary inline execution stores a JSON outcome; supervised workers
    // retain the existing human-readable FSCK report instead.
    let database = Connection::open(root.join("metadata.sqlite3"))?;
    let inline_report: Vec<u8> = database.query_row(
        "SELECT payload FROM shardline_task_artifacts_v1
         WHERE artifact_key GLOB '*/artifacts/*'
           AND CAST(payload AS TEXT) LIKE 'maintenance.success: false%'
         LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    let outcome: serde_json::Value = serde_json::from_slice(
        inline_report
            .split(|byte| *byte == b'\n')
            .nth(1)
            .ok_or("missing inline FSCK outcome")?,
    )?;
    assert_eq!(outcome.get("exit_code"), Some(&serde_json::json!(1)));
    assert_eq!(outcome.get("schema"), Some(&serde_json::json!(1)));
    let state = sandbox.path().join("maintenance");
    let result = shardline_test_support::native_client_output(
        &mut command(&state, &root, "fsck"),
        Duration::from_secs(60),
    )?;
    assert_eq!(result.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("maintenance.status: completed"),
        "completed scan must retain its completion diagnostic"
    );
    let directory = journal(&state)
        .parent()
        .ok_or("state directory")?
        .to_path_buf();
    let proof =
        MaintenanceArtifactProof::from_json(&fs::read(directory.join("result-proof.json"))?)?;
    let artifact = directory
        .join("artifacts")
        .join(hex::encode(proof.artifact.digest.0));
    let report = fs::read(&artifact)?;
    proof.artifact.verify_materialized(report.as_slice())?;
    assert!(report.starts_with(b"maintenance.success: false\n"));
    assert_eq!(
        shardline_test_support::native_client_output(
            &mut command(&state, &root, "fsck"),
            Duration::from_secs(60),
        )?
        .status
        .code(),
        Some(1)
    );
    assert_eq!(
        fs::read(artifact)?,
        report,
        "replay must preserve the failed result"
    );
    Ok(())
}
