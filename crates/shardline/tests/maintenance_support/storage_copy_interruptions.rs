use super::super::*;
use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunIdentity, TaskRunState};

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[test]
fn killed_storage_copy_preserves_effects_without_completion_or_inspection_replay() -> Result<()> {
    for wait_for_report in [false, true] {
        let source = tempfile::tempdir()?;
        let destination = tempfile::tempdir()?;
        let scratch = tempfile::tempdir()?;
        let payload = vec![0x5a; 128 * 1024];
        fs::create_dir_all(source.path().join("chunks/copy-cut"))?;
        // Enough independent atomic writes to observe admission before completion.
        // The journal lock below, rather than a sleep, holds the completion phase.
        for ordinal in 0..256 {
            fs::write(
                source.path().join(format!("chunks/copy-cut/{ordinal:04}")),
                &payload,
            )?;
        }
        let source_config = shardline::load_server_config(Some(source.path()), None)?;
        let source_deployment =
            shardline_server::task_tracking::deployment_fingerprint(&source_config)?;
        let state = scratch.path().join(format!(
            "shardline-maintenance-{}-{}",
            rustix::process::geteuid().as_raw(),
            hex::encode(source_deployment.0)
        ));
        let child = Command::new(binary())
            .env_clear()
            .env("TMPDIR", scratch.path())
            .current_dir(source.path())
            .args(["storage", "migrate", "--from", "local", "--from-root"])
            .arg(source.path())
            .args(["--to", "local", "--to-root"])
            .arg(destination.path())
            .stdout(fs::File::create(scratch.path().join("cli.stdout"))?)
            .stderr(fs::File::create(scratch.path().join("cli.stderr"))?)
            .spawn()?;
        let mut child = StopOnDrop(child);
        let mut admitted = None;
        wait_for(|| {
            admitted = admitted_inline_directory(&state);
            admitted.is_some() || child.0.try_wait().ok().flatten().is_some()
        })?;
        let directory = admitted.ok_or("copy finished before the admission fault was installed")?;
        let identity: TaskRunIdentity =
            serde_json::from_slice(&fs::read(directory.join("identity.json"))?)?;
        assert_eq!(identity.owner.pid, child.0.id());
        let journal = Connection::open_with_flags(
            directory.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        journal.execute_batch("BEGIN IMMEDIATE")?;
        assert_eq!(
            journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get::<_, u32>(0)
            })?,
            3
        );
        let mut copied = None;
        wait_for(|| {
            copied = fs::read_dir(destination.path().join("chunks/copy-cut"))
                .ok()
                .and_then(|entries| {
                    entries.flatten().find_map(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .filter(|name| {
                                name.len() == 4 && name.bytes().all(|byte| byte.is_ascii_digit())
                            })
                            .map(|_| entry.path())
                    })
                });
            copied.is_some() || child.0.try_wait().ok().flatten().is_some()
        })?;
        let copied = copied.ok_or("copy made no observed payload mutation")?;
        assert_eq!(fs::read(&copied)?, payload);
        let report = format!(
            "maintenance.success: true\n{}",
            serde_json::json!({"schema": 1, "exit_code": 0})
        );
        let artifact = directory
            .join("artifacts")
            .join(hex::encode(Digest::of(report.as_bytes()).0));
        if wait_for_report {
            wait_for(|| artifact.exists() || child.0.try_wait().ok().flatten().is_some())?;
            assert_eq!(fs::read(&artifact)?, report.as_bytes());
            assert!(
                fs::read_to_string(scratch.path().join("cli.stdout"))?
                    .contains("inserted_objects: 256")
            );
        }
        assert!(
            child.0.try_wait()?.is_none(),
            "{}",
            String::from_utf8_lossy(&fs::read(scratch.path().join("cli.stderr"))?)
        );
        child.0.kill()?;
        let _status = child.0.wait()?;
        if !wait_for_report {
            assert!(
                !artifact.exists(),
                "copy reached report before the mutation-phase kill"
            );
        }
        journal.execute_batch("ROLLBACK")?;
        let config = shardline::load_server_config(Some(destination.path()), None)?;
        let store = TaskEvidenceStore::from_config(&config)?;
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
        let tasks = store.summaries(deployment, None, 100)?;
        assert_eq!(tasks.len(), 1);
        let task = tasks.first().ok_or("missing interrupted copy")?;
        assert_eq!(task.id, identity.id);
        assert_eq!(task.state, TaskRunState::NeedsReconciliation);
        assert!(
            store
                .read(&format!("runs/{}/records/0000000003", identity.id), 65536)?
                .is_none()
        );
        fs::write(
            source.path().join("chunks/copy-cut/later"),
            b"must not be copied by inspection",
        )?;
        let history = Command::new(binary())
            .env_clear()
            .env("TMPDIR", scratch.path())
            .current_dir(destination.path())
            .args(["history", "--root"])
            .arg(destination.path())
            .output()?;
        assert!(history.status.success());
        let page: serde_json::Value = serde_json::from_slice(&history.stdout)?;
        let inspected = page
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .ok_or("missing history tasks")?;
        assert_eq!(inspected.len(), 1);
        let inspected = inspected.first().ok_or("missing inspected copy")?;
        assert_eq!(
            inspected.get("id").and_then(serde_json::Value::as_str),
            Some(identity.id.as_str())
        );
        assert_eq!(
            inspected.get("state").and_then(serde_json::Value::as_str),
            Some("needs_reconciliation")
        );
        assert!(!destination.path().join("chunks/copy-cut/later").exists());
        assert_eq!(fs::read(copied)?, payload);
        assert_eq!(store.prune_completed(deployment, u64::MAX)?, 0);
        assert_eq!(
            journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get::<_, u32>(0)
            })?,
            3
        );
    }
    Ok(())
}
