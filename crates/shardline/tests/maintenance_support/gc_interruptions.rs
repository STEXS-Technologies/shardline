use super::super::*;

#[test]
fn killed_gc_after_domain_mutation_or_report_never_claims_completion_or_replays() -> Result<()> {
    use shardline_index::{LifecycleStore, LocalIndexStore};
    use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunIdentity, TaskRunState};
    use shardline_storage::ObjectKey;
    struct StopOnDrop(std::process::Child);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            let _kill = self.0.kill();
            let _wait = self.0.wait();
        }
    }
    for wait_for_report in [false, true] {
        let root = tempfile::tempdir()?;
        let hash = "dd".repeat(32);
        let key = ObjectKey::parse(&format!("dd/{hash}"))?;
        fs::create_dir_all(root.path().join("chunks/dd"))?;
        let orphan = root.path().join("chunks").join(key.as_str());
        fs::write(&orphan, b"orphan before interrupted GC")?;
        let index = LocalIndexStore::open(root.path().to_path_buf());
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
            .args(["gc", "--mark", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(root.path().join("cli.stdout"))?)
            .stderr(fs::File::create(root.path().join("cli.stderr"))?)
            .spawn()?;
        let mut child = StopOnDrop(child);
        let state = root.path().join(".shardline-maintenance");
        let mut admitted = None;
        wait_for(|| {
            admitted = admitted_inline_directory(&state);
            admitted.is_some() || child.0.try_wait().ok().flatten().is_some()
        })?;
        let directory = admitted.ok_or("GC did not reach native admission")?;
        let identity: TaskRunIdentity =
            serde_json::from_slice(&fs::read(directory.join("identity.json"))?)?;
        assert_eq!(identity.owner.pid, child.0.id());
        let config = shardline_server::ServerConfig::new(
            "127.0.0.1:8080".parse()?,
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).ok_or("chunk size")?,
        );
        let store = TaskEvidenceStore::from_config(&config)?;
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
        // Pause completion recording, not domain work. A real SQLite writer
        // holds the same journal arbitration used by the production CLI.
        let journal = Connection::open_with_flags(
            directory.join("tasks.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        journal.execute_batch("BEGIN IMMEDIATE")?;
        barrier.unlock()?;
        wait_for(|| {
            index.quarantine_candidate(&key).ok().flatten().is_some()
                || child.0.try_wait().ok().flatten().is_some()
        })?;
        let candidate = index
            .quarantine_candidate(&key)?
            .ok_or("GC did not mark orphan")?;
        let report = format!(
            "maintenance.success: true\n{}",
            serde_json::json!({"schema": 1, "exit_code": 0})
        );
        let report_path = directory
            .join("artifacts")
            .join(hex::encode(Digest::of(report.as_bytes()).0));
        if wait_for_report {
            wait_for(|| report_path.exists() || child.0.try_wait().ok().flatten().is_some())?;
            assert_eq!(fs::read(&report_path)?, report.as_bytes());
            assert!(
                fs::read_to_string(root.path().join("cli.stdout"))?
                    .contains("new_quarantine_candidates: 1")
            );
        }
        assert!(
            child.0.try_wait()?.is_none(),
            "{}",
            String::from_utf8_lossy(&fs::read(root.path().join("cli.stderr"))?)
        );
        child.0.kill()?;
        let _status = child.0.wait()?;
        journal.execute_batch("ROLLBACK")?;
        let interrupted = store.summaries(deployment, None, 100)?;
        assert_eq!(interrupted.len(), 1);
        assert!(
            interrupted
                .iter()
                .all(|task| task.id == identity.id
                    && task.state == TaskRunState::NeedsReconciliation)
        );
        let second_key = ObjectKey::parse(&format!("ee/{}", "ee".repeat(32)))?;
        fs::create_dir_all(root.path().join("chunks/ee"))?;
        fs::write(
            root.path().join("chunks").join(second_key.as_str()),
            b"orphan created after interruption",
        )?;
        // A fresh diagnostic exports evidence only. Replaying the old GC would
        // quarantine this second orphan, which was absent from the original run.
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
        assert_eq!(index.quarantine_candidate(&key)?, Some(candidate.clone()));
        assert!(index.quarantine_candidate(&second_key)?.is_none());
        assert_eq!(fs::read(&orphan)?, b"orphan before interrupted GC");
        assert_eq!(
            journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
                .get::<_, u32>(0))?,
            3
        );
        let tasks = store.summaries(deployment, None, 100)?;
        assert_eq!(tasks.len(), 2);
        assert!(
            tasks
                .iter()
                .any(|task| task.id == identity.id
                    && task.state == TaskRunState::NeedsReconciliation)
        );
        assert!(
            tasks
                .iter()
                .any(|task| task.id != identity.id && task.state == TaskRunState::Completed)
        );
        let fresh_gc = Command::new(binary())
            .env_clear()
            .current_dir(root.path())
            .args(["gc", "--mark", "--root"])
            .arg(root.path())
            .output()?;
        assert!(
            fresh_gc.status.success(),
            "{}",
            String::from_utf8_lossy(&fresh_gc.stderr)
        );
        assert!(String::from_utf8_lossy(&fresh_gc.stdout).contains("new_quarantine_candidates: 1"));
        assert_eq!(index.quarantine_candidate(&key)?, Some(candidate));
        assert!(index.quarantine_candidate(&second_key)?.is_some());
        assert_eq!(fs::read(&orphan)?, b"orphan before interrupted GC");
        assert_eq!(
            fs::read(root.path().join("chunks").join(second_key.as_str()))?,
            b"orphan created after interruption"
        );
        assert_eq!(
            journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
                .get::<_, u32>(0))?,
            3
        );
        let refreshed_tasks = store.summaries(deployment, None, 100)?;
        assert_eq!(refreshed_tasks.len(), 3);
        assert!(
            refreshed_tasks
                .iter()
                .any(|task| task.id == identity.id
                    && task.state == TaskRunState::NeedsReconciliation)
        );
        assert_eq!(
            refreshed_tasks
                .iter()
                .filter(|task| task.state == TaskRunState::Completed)
                .count(),
            2
        );
    }
    Ok(())
}
