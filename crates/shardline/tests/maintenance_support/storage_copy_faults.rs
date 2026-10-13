use super::super::*;
use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunState};

#[test]
fn copied_payload_survives_report_and_acknowledgement_faults_without_inspection_replay()
-> Result<()> {
    for deny_report in [true, false] {
        let source = tempfile::tempdir()?;
        let destination = tempfile::tempdir()?;
        fs::create_dir_all(source.path().join("chunks/test"))?;
        let bytes = b"copy before result recording fails";
        fs::write(source.path().join("chunks/test/first"), bytes)?;
        let config = shardline::load_server_config(Some(destination.path()), None)?;
        let store = TaskEvidenceStore::from_config(&config)?;
        store.publish("runs/fixture/blobs/seed", b"unrelated evidence")?;
        let metadata = Connection::open(destination.path().join("metadata.sqlite3"))?;
        let predicate = if deny_report {
            "NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/artifacts/*'
             AND CAST(NEW.payload AS TEXT) LIKE 'maintenance.success:%'"
        } else {
            "NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/records/0000000003'"
        };
        metadata.execute_batch(&format!(
            "CREATE TRIGGER deny_copy_result BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN {predicate} BEGIN SELECT RAISE(ABORT, 'owned copy result fault'); END;"
        ))?;
        let output = Command::new(binary())
            .env_clear()
            .current_dir(source.path())
            .args(["storage", "migrate", "--from", "local", "--from-root"])
            .arg(source.path())
            .args(["--to", "local", "--to-root"])
            .arg(destination.path())
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("inserted_objects: 1"));
        assert_eq!(
            fs::read(destination.path().join("chunks/test/first"))?,
            bytes
        );
        let identities = maintenance_identities(destination.path())?;
        assert_eq!(identities.len(), 1);
        let identity = identities.first().ok_or("missing admitted copy identity")?;
        for sequence in 0..=2 {
            assert!(
                store
                    .read(
                        &format!("runs/{}/records/{sequence:010}", identity.id),
                        65536
                    )?
                    .is_some()
            );
        }
        assert!(
            store
                .read(&format!("runs/{}/records/0000000003", identity.id), 65536)?
                .is_none()
        );
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
        let tasks = store.summaries(deployment, None, 100)?;
        assert_eq!(tasks.len(), 1);
        assert!(
            !tasks
                .iter()
                .any(|task| matches!(task.state, TaskRunState::Completed | TaskRunState::Failed))
        );
        metadata.execute_batch("DROP TRIGGER deny_copy_result")?;
        fs::write(
            source.path().join("chunks/test/later"),
            b"must not be copied by inspection",
        )?;
        let history = Command::new(binary())
            .env_clear()
            .current_dir(destination.path())
            .args(["history", "--root"])
            .arg(destination.path())
            .output()?;
        assert!(
            history.status.success(),
            "{}",
            String::from_utf8_lossy(&history.stderr)
        );
        let page: serde_json::Value = serde_json::from_slice(&history.stdout)?;
        let inspected = page
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .ok_or("history did not return a task page")?;
        assert_eq!(inspected.len(), 1);
        let task = inspected.first().ok_or("missing inspected copy")?;
        assert_eq!(
            task.get("id").and_then(serde_json::Value::as_str),
            Some(identity.id.as_str())
        );
        assert_eq!(
            task.get("state").and_then(serde_json::Value::as_str),
            Some("needs_reconciliation")
        );
        assert!(
            task.get("next_step")
                .and_then(serde_json::Value::as_str)
                .is_some_and(
                    |step| step.contains("Inspect") && step.contains("original command logs")
                )
        );
        assert!(!destination.path().join("chunks/test/later").exists());
        assert_eq!(
            fs::read(destination.path().join("chunks/test/first"))?,
            bytes
        );
        assert_eq!(
            store.read("runs/fixture/blobs/seed", 100)?,
            Some(b"unrelated evidence".to_vec())
        );
        assert_eq!(store.prune_completed(deployment, u64::MAX)?, 0);
    }
    Ok(())
}
