use super::super::*;
use shardline_index::{LocalRecordStore, RecordMutation, RecordTraversal};
use shardline_server::LocalBackend;

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[test]
fn rebuild_sigterm_preserves_partial_heads_and_fresh_record_discovery() -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    let runtime = tokio::runtime::Runtime::new()?;
    let backend = runtime.block_on(LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".into(),
        std::num::NonZeroUsize::new(128).ok_or("chunk size")?,
    ))?;
    let records = LocalRecordStore::open(root.path().to_path_buf());
    for (name, body) in [
        ("first-file", b"first body".as_slice()),
        ("second-file", b"second body"),
        ("protected-file", b"protected body"),
    ] {
        runtime.block_on(backend.upload_file(name, body.to_vec().into(), None))?;
    }
    let metadata = Connection::open(root.path().join("metadata.sqlite3"))?;
    let read_head = |name: &str| -> Result<shardline_index::FileRecord> {
        let json: String = metadata.query_row(
            "SELECT record FROM shardline_file_records WHERE record_kind='latest' AND file_id=?1",
            [name],
            |row| row.get(0),
        )?;
        Ok(serde_json::from_str(&json)?)
    };
    let first = read_head("first-file")?;
    let second = read_head("second-file")?;
    for record in [&first, &second] {
        let locator = RecordTraversal::latest_record_locator(&records, record);
        runtime.block_on(RecordMutation::delete_record_locator(&records, &locator))?;
    }
    metadata.busy_timeout(Duration::ZERO)?;
    metadata.execute_batch(
        "CREATE TRIGGER hold_rebuild_head BEFORE INSERT ON shardline_file_records
        WHEN NEW.record_kind = 'latest' AND
          (SELECT count(*) FROM shardline_file_records WHERE record_kind = 'latest') >= 2
        BEGIN SELECT (WITH RECURSIVE pause(counter) AS
          (VALUES(0) UNION ALL SELECT counter+1 FROM pause WHERE counter<1000000000)
          SELECT sum(counter) FROM pause); END;",
    )?;
    let version_snapshot = || -> Result<Vec<(String, String)>> {
        let mut statement = metadata.prepare("SELECT record_key, record FROM shardline_file_records WHERE record_kind = 'version' ORDER BY record_key")?;
        let values = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(values.collect::<std::result::Result<_, _>>()?)
    };
    let versions_before = version_snapshot()?;
    let mut child = StopOnDrop(
        Command::new(binary())
            .env_clear()
            .current_dir(root.path())
            .args(["index", "rebuild", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("stdout"))?)
            .stderr(fs::File::create(scratch.path().join("stderr"))?)
            .spawn()?,
    );
    let latest_count = || {
        metadata.query_row(
            "SELECT count(*) FROM shardline_file_records WHERE record_kind = 'latest'",
            [],
            |row| row.get::<_, u32>(0),
        )
    };
    wait_for(|| {
        if latest_count().ok() == Some(2) {
            if metadata.execute_batch("BEGIN IMMEDIATE").is_err() {
                return true;
            }
            let _rollback = metadata.execute_batch("ROLLBACK");
        }
        child.0.try_wait().ok().flatten().is_some()
    })?;
    assert!(child.0.try_wait()?.is_none());
    let identities = maintenance_identities(root.path())?;
    assert_eq!(identities.len(), 1);
    let identity = identities.first().ok_or("missing rebuild identity")?;
    assert_eq!(identity.operation, "IndexRebuild");
    assert_eq!(identity.owner.pid, child.0.id());
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(child.0.wait()?.signal(), Some(15));
    metadata.execute_batch("DROP TRIGGER hold_rebuild_head")?;
    assert_eq!(latest_count()?, 2);
    assert_eq!(version_snapshot()?, versions_before);
    let restored = [
        runtime.block_on(RecordTraversal::read_latest_record_bytes(&records, &first))?,
        runtime.block_on(RecordTraversal::read_latest_record_bytes(&records, &second))?,
    ];
    assert_eq!(restored.iter().filter(|head| head.is_some()).count(), 1);
    for (name, body, record) in [
        ("first-file", b"first body".as_slice(), &first),
        ("second-file", b"second body", &second),
    ] {
        assert_eq!(
            runtime.block_on(backend.download_file(name, Some(&record.content_hash), None))?,
            body
        );
    }
    assert_eq!(
        runtime.block_on(backend.download_file("protected-file", None, None))?,
        b"protected body"
    );
    let workspace = root
        .path()
        .join(".shardline-maintenance")
        .join(hex::encode(Digest::of(identity.id.as_bytes()).0));
    let native = Connection::open_with_flags(
        workspace.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    assert_eq!(
        native.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        3
    );
    let before = fs::read(workspace.join("tasks.sqlite"))?;
    let before_wal = fs::read(workspace.join("tasks.sqlite-wal")).ok();
    let inspection = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(inspection.status.success());
    let page: serde_json::Value = serde_json::from_slice(&inspection.stdout)?;
    assert_eq!(
        page.pointer("/tasks/0/state")
            .and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    assert_eq!(latest_count()?, 2);
    runtime.block_on(backend.upload_file("later-file", b"later body".to_vec().into(), None))?;
    let later = read_head("later-file")?;
    runtime.block_on(RecordMutation::delete_record_locator(
        &records,
        &RecordTraversal::latest_record_locator(&records, &later),
    ))?;
    let fresh = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    assert!(String::from_utf8_lossy(&fresh.stdout).contains("rebuilt_latest_records: 2"));
    assert_eq!(latest_count()?, 4);
    for (name, body) in [
        ("first-file", b"first body".as_slice()),
        ("second-file", b"second body"),
        ("protected-file", b"protected body"),
        ("later-file", b"later body"),
    ] {
        assert_eq!(
            runtime.block_on(backend.download_file(name, None, None))?,
            body
        );
    }
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        before_wal
    );
    let refreshed = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(refreshed.status.success());
    let refreshed_page: serde_json::Value = serde_json::from_slice(&refreshed.stdout)?;
    let tasks = refreshed_page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or("missing rebuild tasks")?;
    assert_eq!(tasks.len(), 2);
    assert_eq!(
        tasks
            .iter()
            .filter(
                |task| task.get("state").and_then(serde_json::Value::as_str) == Some("completed")
            )
            .count(),
        1
    );
    let interrupted = tasks
        .iter()
        .find(|task| {
            task.get("id").and_then(serde_json::Value::as_str) == Some(identity.id.as_str())
        })
        .ok_or("missing interrupted rebuild")?;
    assert_eq!(
        interrupted.get("state").and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    Ok(())
}
