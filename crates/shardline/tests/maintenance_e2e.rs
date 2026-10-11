#![cfg(target_os = "linux")]
#![allow(clippy::panic_in_result_fn)]
use penelope_core::task::Digest;
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

fn journal(state: &Path) -> std::path::PathBuf {
    state
        .join(hex::encode(Digest::of(b"test-task").0))
        .join("tasks.sqlite")
}

fn binary() -> std::ffi::OsString {
    std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into())
}

#[test]
fn failed_tracking_preparation_is_visible_and_preserves_original_validation_status() -> Result<()> {
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
    assert!(stderr.contains("maintenance tracking could not initialize"));
    assert!(
        stderr.lines().count() >= 2,
        "retain the original diagnostic after the tracking warning: {stderr}"
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
    use penelope_core::task::{TaskProjection, TaskRecord, TaskStatus};
    use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunIdentity};
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
        let manifests = fs::read_dir(root.join("chunks/_shardline_tasks/v1/manifests"))?
            .collect::<std::io::Result<Vec<_>>>()?;
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
        for manifest in manifests {
            let identity: TaskRunIdentity = serde_json::from_slice(&fs::read(manifest.path())?)?;
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
                &statechronicle_domain::intent::KeyId(format!(
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
        assert_eq!(
            fs::read_dir(root.join(".shardline-maintenance"))?.count(),
            0,
            "completed local scratch should be cleaned"
        );
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
        fs::create_dir_all(root.join("chunks/_shardline_tasks/v1"))?;
        fs::write(
            root.join("chunks/_shardline_tasks/v1/manifests"),
            b"blocked publication",
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
        fs::remove_file(root.join("chunks/_shardline_tasks/v1/manifests"))?;
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
        assert_eq!(
            fs::read_dir(root.join("chunks/_shardline_tasks/v1/manifests"))?.count(),
            2
        );
        assert_eq!(
            fs::read_dir(root.join(".shardline-maintenance"))?.count(),
            0
        );
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
    assert_eq!(
        fs::read_dir(root.join("chunks/_shardline_tasks/v1/manifests"))?.count(),
        1
    );
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
        let Ok(entries) = fs::read_dir(&state) else {
            return false;
        };
        for entry in entries.flatten() {
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
                interrupted = Some(entry.path());
                return true;
            }
        }
        false
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
    let manifests = fs::read_dir(root.path().join("chunks/_shardline_tasks/v1/manifests"))?
        .collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(manifests.len(), 1);
    let identity: shardline_server::task_tracking::TaskRunIdentity =
        serde_json::from_slice(&fs::read(manifests.first().ok_or("missing run")?.path())?)?;
    assert_eq!(identity.owner.pid, pid);
    assert_eq!(
        identity.execution,
        shardline_server::task_tracking::TaskExecutionMode::Inline
    );
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
        Ok(command.output()?)
    };
    let dry = invoke(true)?;
    assert!(
        dry.status.success(),
        "{}",
        String::from_utf8_lossy(&dry.stderr)
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
    assert!(
        real.status.success(),
        "{}",
        String::from_utf8_lossy(&real.stderr)
    );
    assert_eq!(
        fs::read(destination.path().join("chunks/test/object"))?,
        bytes
    );
    assert!(!source.path().join("chunks/_shardline_tasks/v1").exists());
    assert_eq!(
        fs::read_dir(
            destination
                .path()
                .join("chunks/_shardline_tasks/v1/manifests")
        )?
        .count(),
        1
    );
    let repeated = invoke(false)?;
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    for output in [dry, real, repeated] {
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("scanned_objects: 1"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    Ok(())
}

#[test]
fn real_workflows_commit_native_checkpoints_and_reuse_completed_history() -> Result<()> {
    for operation in ["fsck", "index-rebuild", "gc", "lifecycle-repair"] {
        let sandbox = tempfile::tempdir()?;
        let root = sandbox.path().join("deployment");
        fs::create_dir(&root)?;
        let state = sandbox.path().join("maintenance");
        let first = command(&state, &root, operation).output()?;
        assert!(
            first.status.success(),
            "{operation}: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        let connection = Connection::open(journal(&state))?;
        let records: u32 =
            connection.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get(0)
            })?;
        let repeated = command(&state, &root, operation).output()?;
        assert!(
            repeated.status.success(),
            "repeat {operation}: {}",
            String::from_utf8_lossy(&repeated.stderr)
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
    assert!(command(&state, &root, "fsck").output()?.status.success());
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
    assert!(!command(&state, &root, "fsck").output()?.status.success());
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
    let concurrent = command(&state, &root, "index-rebuild").output()?;
    assert!(!concurrent.status.success());
    controller.kill()?;
    let _status = controller.wait()?;
    let recovery = command(&state, &root, "index-rebuild").output()?;
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
        !command(&root.join("state"), &root, "fsck")
            .output()?
            .status
            .success()
    );
    assert!(!root.join("state").exists());
    let alias = sandbox.path().join("alias");
    fs::create_dir(&alias)?;
    assert!(
        !command(&alias.join("../deployment/state"), &root, "fsck")
            .output()?
            .status
            .success()
    );
    assert!(!root.join("state").exists());
    let worker = Command::new(binary())
        .args(["maintenance-worker", "--journal"])
        .arg(root.join("missing.sqlite"))
        .args(["--scope", "00", "--generation", "1"])
        .output()?;
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
    let output = command(&state, &root, "fsck")
        .arg("--config")
        .arg(&config)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(&config, "[server]\nfrontends = [\"xet\"]\n")?;
    let changed = command(&state, &root, "fsck")
        .arg("--config")
        .arg(&config)
        .output()?;
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
    let first = command(&state, &root, "storage-migrate")
        .args(["--from", "local", "--to", "local", "--to-root"])
        .arg(&destination)
        .output()?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
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
    assert!(!report.contains("inserted_objects: 0"), "{report}");
    assert!(destination.join("chunks").is_dir());
    let other = sandbox.path().join("other");
    fs::create_dir(&other)?;
    assert!(
        !command(&state, &root, "storage-migrate")
            .args(["--from", "local", "--to", "local", "--to-root"])
            .arg(&other)
            .output()?
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
    let ordinary = Command::new(binary())
        .env_clear()
        .current_dir(&root)
        .args(["fsck", "--root"])
        .arg(&root)
        .output()?;
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
    let state = sandbox.path().join("maintenance");
    let result = command(&state, &root, "fsck").output()?;
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("maintenance.status: completed"),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!command(&state, &root, "fsck").output()?.status.success());
    Ok(())
}
