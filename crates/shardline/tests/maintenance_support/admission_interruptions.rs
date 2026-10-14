use super::super::*;
use shardline_server::task_tracking::TaskRunIdentity;

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[test]
fn sigterm_before_copy_intent_preserves_pending_archive_without_destination_writes() -> Result<()> {
    verify_admission_cut(0, AdmissionOperation::Copy)
}

#[test]
fn sigterm_after_copy_intent_preserves_pending_archive_without_destination_writes() -> Result<()> {
    verify_admission_cut(1, AdmissionOperation::Copy)
}

#[test]
fn sigterm_after_copy_started_preserves_unresolved_archive_without_destination_writes() -> Result<()>
{
    verify_admission_cut(2, AdmissionOperation::Copy)
}

#[derive(Clone, Copy)]
enum AdmissionOperation {
    Copy,
    Fsck,
    Rebuild,
    GcMark,
    LifecycleRepair,
    DefaultRepair,
}

impl AdmissionOperation {
    const fn arguments(self) -> &'static [&'static str] {
        match self {
            Self::Copy => &["storage", "migrate", "--from", "local", "--from-root"],
            Self::Fsck => &["fsck", "--root"],
            Self::Rebuild => &["index", "rebuild", "--root"],
            Self::GcMark => &["gc", "--mark", "--root"],
            Self::LifecycleRepair => &["repair", "lifecycle", "--root"],
            Self::DefaultRepair => &["repair", "--root"],
        }
    }
    const fn identity_kind(self) -> &'static str {
        match self {
            Self::Copy => "StorageMigration",
            Self::Fsck => "Fsck",
            Self::Rebuild => "IndexRebuild",
            Self::GcMark => "GarbageCollection",
            Self::LifecycleRepair | Self::DefaultRepair => "LifecycleRepair",
        }
    }
}

#[test]
fn ordinary_commands_preserve_native_admission_cuts_without_application_replay() -> Result<()> {
    for operation in [
        AdmissionOperation::Fsck,
        AdmissionOperation::Rebuild,
        AdmissionOperation::GcMark,
        AdmissionOperation::LifecycleRepair,
        AdmissionOperation::DefaultRepair,
    ] {
        for records in 0..=2 {
            verify_admission_cut(records, operation)?;
        }
    }
    Ok(())
}

fn verify_admission_cut(records: u32, operation: AdmissionOperation) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    fs::create_dir_all(source.path().join("chunks/admission-cut"))?;
    fs::write(
        source.path().join("chunks/admission-cut/first"),
        b"not yet copied",
    )?;
    fs::create_dir_all(destination.path().join("chunks/admission-cut"))?;
    fs::write(
        destination.path().join("chunks/admission-cut/sentinel"),
        b"preserve existing payload",
    )?;
    let index = shardline_index::LocalIndexStore::new(destination.path().to_path_buf())?;
    index.publish_task_artifact("_shardline_tasks/v1/fixture-admission-anchor", b"fixture")?;
    let metadata = Connection::open(destination.path().join("metadata.sqlite3"))?;
    if records == 0 {
        metadata.execute_batch("BEGIN IMMEDIATE")?;
    } else {
        let pattern = if records == 1 {
            "*/blobs/*"
        } else {
            "*/records/0000000001"
        };
        // Hold the evidence export after the selected native commit. The large
        // query runs only in the owned CLI and is terminated with that process;
        // no production pause hook or extra worker is required.
        metadata.execute_batch(&format!(
            "CREATE TRIGGER hold_admission_export BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN NEW.artifact_key GLOB '{pattern}' BEGIN
             SELECT (WITH RECURSIVE pause(counter) AS
               (VALUES(0) UNION ALL SELECT counter+1 FROM pause WHERE counter<1000000000)
               SELECT sum(counter) FROM pause); END;"
        ))?;
    }
    let copying = matches!(operation, AdmissionOperation::Copy);
    let work_root = if copying {
        source.path()
    } else {
        destination.path()
    };
    let config = shardline::load_server_config(Some(work_root), None)?;
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config)?;
    let state = if copying {
        scratch.path().join(format!(
            "shardline-maintenance-{}-{}",
            rustix::process::geteuid().as_raw(),
            hex::encode(deployment.0)
        ))
    } else {
        destination.path().join(".shardline-maintenance")
    };
    let mut command = Command::new(binary());
    command
        .env_clear()
        .env("TMPDIR", scratch.path())
        .current_dir(work_root)
        .args(operation.arguments())
        .arg(work_root)
        .stdout(fs::File::create(scratch.path().join("cli.stdout"))?)
        .stderr(fs::File::create(scratch.path().join("cli.stderr"))?);
    if copying {
        command
            .args(["--to", "local", "--to-root"])
            .arg(destination.path());
    }
    let mut child = StopOnDrop(command.spawn()?);
    let mut workspace = None;
    wait_for(|| {
        workspace = fs::read_dir(&state).ok().and_then(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .find(|path| path.join("identity.json").is_file())
        });
        workspace.is_some() || child.0.try_wait().ok().flatten().is_some()
    })?;
    let workspace = workspace.ok_or("copy did not reach manifest publication")?;
    let identity_bytes = fs::read(workspace.join("identity.json"))?;
    let identity: TaskRunIdentity = serde_json::from_slice(&identity_bytes)?;
    assert_eq!(identity.owner.pid, child.0.id());
    assert_eq!(identity.operation, operation.identity_kind());
    let journal = Connection::open_with_flags(
        workspace.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    wait_for(|| {
        journal
            .query_row("SELECT count(*) FROM penelope_records_v1", [], |row| {
                row.get::<_, u32>(0)
            })
            .ok()
            == Some(records)
            || child.0.try_wait().ok().flatten().is_some()
    })?;
    assert_eq!(
        journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        records
    );
    assert!(child.0.try_wait()?.is_none());
    assert!(
        !destination
            .path()
            .join("chunks/admission-cut/first")
            .exists()
    );
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(child.0.wait()?.signal(), Some(15));
    if records == 0 {
        metadata.execute_batch("ROLLBACK")?;
    } else {
        metadata.execute_batch("DROP TRIGGER hold_admission_export")?;
    }
    let database_before = fs::read(workspace.join("tasks.sqlite"))?;
    let wal_before = fs::read(workspace.join("tasks.sqlite-wal")).ok();
    fs::write(
        source.path().join("chunks/admission-cut/later"),
        b"inspection must not copy",
    )?;
    let history = Command::new(binary())
        .env_clear()
        .current_dir(source.path())
        .args(["history", "--archive", "--root"])
        .arg(&workspace)
        .output()?;
    assert!(
        history.status.success(),
        "{}",
        String::from_utf8_lossy(&history.stderr)
    );
    let page: serde_json::Value = serde_json::from_slice(&history.stdout)?;
    assert_eq!(
        page.get("tasks")
            .and_then(serde_json::Value::as_array)
            .ok_or("missing tasks")?
            .len(),
        1
    );
    assert_eq!(
        page.pointer("/tasks/0/state")
            .and_then(serde_json::Value::as_str),
        Some(if records < 2 {
            "pending"
        } else {
            "needs_reconciliation"
        })
    );
    assert_eq!(
        page.pointer("/tasks/0/id")
            .and_then(serde_json::Value::as_str),
        Some(identity.id.as_str())
    );
    assert_eq!(
        page.pointer("/tasks/0/owner/pid")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(child.0.id()))
    );
    assert_eq!(
        page.get("native_journal")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        page.pointer("/tasks/0/next_step")
            .is_some_and(serde_json::Value::is_string)
    );
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, database_before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        wal_before
    );
    assert_eq!(fs::read(workspace.join("identity.json"))?, identity_bytes);
    assert_eq!(
        journal.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        records
    );
    assert!(
        !destination
            .path()
            .join("chunks/admission-cut/first")
            .exists()
    );
    assert!(
        !destination
            .path()
            .join("chunks/admission-cut/later")
            .exists()
    );
    assert!(fs::read(scratch.path().join("cli.stdout"))?.is_empty());
    assert_eq!(
        fs::read(destination.path().join("chunks/admission-cut/sentinel"))?,
        b"preserve existing payload"
    );
    Ok(())
}
