use super::super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn native_archive_cli_verifies_unexported_result_without_replaying_work() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    let config = shardline::load_server_config(Some(&root), None)?;
    let store = shardline_server::task_tracking::TaskEvidenceStore::from_config(&config)?;
    store.publish("runs/fixture/blobs/seed", b"unrelated evidence")?;
    let metadata = Connection::open(root.join("metadata.sqlite3"))?;
    metadata.execute_batch(
        "CREATE TRIGGER deny_archive_export BEFORE INSERT ON shardline_task_artifacts_v1
        WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/manifests/*'
        BEGIN SELECT RAISE(ABORT, 'owned archive export fault'); END;",
    )?;
    let completed = Command::new(binary())
        .env_clear()
        .current_dir(sandbox.path())
        .args(["index", "rebuild", "--root"])
        .arg(&root)
        .output()?;
    assert!(completed.status.success(), "{completed:?}");
    let workspace = fs::read_dir(root.join(".shardline-maintenance"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.join("identity.json").exists())
        .ok_or("missing retained workspace")?;
    let path = workspace.join("tasks.sqlite");
    let warnings = String::from_utf8_lossy(&completed.stderr);
    let retained = warnings
        .lines()
        .find(|line| line.contains("shared task evidence unavailable; retaining local evidence"))
        .ok_or("missing retained-evidence warning")?;
    assert!(
        retained.ends_with(&workspace.display().to_string()),
        "the warning must name the directory accepted by history --archive: {retained}"
    );
    let before = fs::read(&path)?;
    let inspect = || {
        Command::new(binary())
            .env_clear()
            .current_dir(sandbox.path())
            .args(["history", "--archive", "--root"])
            .arg(&workspace)
            .output()
    };
    let output = inspect()?;
    assert!(output.status.success(), "{output:?}");
    let page: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(page.get("native_journal"), Some(&serde_json::json!(true)));
    assert_eq!(
        page.pointer("/tasks/0/state"),
        Some(&serde_json::json!("completed"))
    );
    assert_eq!(fs::read(&path)?, before);
    // A saved signed result cannot establish completion with damaged bytes.
    let stream = fs::read_dir(workspace.join("artifacts"))?
        .next()
        .ok_or("missing saved artifact")??
        .path();
    fs::write(&stream, b"corrupt saved output")?;
    let rejected = inspect()?;
    assert!(!rejected.status.success());
    assert_eq!(fs::read(&stream)?, b"corrupt saved output");
    assert_eq!(fs::read(&path)?, before);
    Ok(())
}

#[test]
fn archive_inspection_requires_explicit_private_root_and_never_initializes_it() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("sentinel"), b"preserved archive bytes")?;
    for arguments in [
        vec!["history", "--archive"],
        vec!["history", "--archive", "--pending-deletes", "--root"],
        vec!["history", "--archive", "--limit", "0", "--root"],
    ] {
        let append_root = arguments.last() == Some(&"--root");
        let mut command = Command::new(binary());
        command.env_clear().current_dir(root.path()).args(arguments);
        if append_root {
            command.arg(root.path());
        }
        assert!(!command.output()?.status.success());
    }
    // An explicitly selected empty archive must not acquire a database or create
    // a payload directory, regardless of whether inspection reports empty/error.
    let _output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--archive", "--root"])
        .arg(root.path())
        .output()?;
    assert!(!root.path().join("metadata.sqlite3").exists());
    assert!(!root.path().join("chunks").exists());
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755))?;
    let denied = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--archive", "--root"])
        .arg(root.path())
        .output()?;
    assert!(!denied.status.success());
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("inspect a private archive as its owner")
    );
    assert_eq!(
        fs::metadata(root.path())?.permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        fs::read(root.path().join("sentinel"))?,
        b"preserved archive bytes"
    );
    assert_eq!(fs::read_dir(root.path())?.count(), 1);
    Ok(())
}
