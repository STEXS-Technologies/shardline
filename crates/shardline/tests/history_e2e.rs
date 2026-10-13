#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use std::process::Command;

#[test]
fn history_command_reads_ordinary_runs_without_private_runner_configuration() {
    let root = tempfile::tempdir().unwrap();
    let binary = std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into());
    let bootstrap = Command::new(&binary)
        .env_clear()
        .current_dir(root.path())
        .args(["fsck", "--root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(bootstrap.status.success(), "{bootstrap:?}");
    let snapshot = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    let artifacts = || {
        let mut query = snapshot.prepare("SELECT artifact_key,payload FROM shardline_task_artifacts_v1 ORDER BY artifact_key").unwrap();
        query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let before = artifacts();
    for _ in 0..2 {
        let output = Command::new(&binary)
            .env_clear()
            .current_dir(root.path())
            .args(["history", "--root"])
            .arg(root.path())
            .args(["--limit", "1"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let page: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(page["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(page["tasks"][0]["state"], "completed");
        assert!(page["tasks"][0]["next_step"].is_string());
        assert_eq!(artifacts(), before);
    }
    let rejected = Command::new(&binary)
        .env_clear()
        .args(["history", "--limit", "0"])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
}

#[test]
fn unavailable_history_is_an_error_and_preserves_existing_metadata() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite3");
    std::fs::write(&path, b"not a database").unwrap();
    let binary = std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into());
    let output = Command::new(binary)
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("does not establish whether work ran")
    );
    assert_eq!(std::fs::read(path).unwrap(), b"not a database");
    assert!(!root.path().join("chunks").exists());
    assert!(!root.path().join(".shardline-maintenance").exists());
}
