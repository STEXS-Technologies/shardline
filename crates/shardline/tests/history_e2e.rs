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

#[cfg(target_os = "linux")]
#[test]
fn missing_wal_sidecars_produce_actionable_errors_without_changing_database_bytes() {
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        rustix::process::geteuid().as_raw(),
        0,
        "run this permissions fixture as an unprivileged user"
    );
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
    let database = root.path().join("metadata.sqlite3");
    let original = std::fs::read(&database).unwrap();
    struct Restore(std::path::PathBuf, std::path::PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _root_restored =
                std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            let _database_restored =
                std::fs::set_permissions(&self.1, std::fs::Permissions::from_mode(0o644));
        }
    }
    let _restore = Restore(root.path().to_path_buf(), database.clone());
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o444)).unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let output = Command::new(binary)
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("SQLite history requires readable"),
        "{output:?}"
    );
    assert!(!root.path().join("metadata.sqlite3-wal").exists());
    assert!(!root.path().join("metadata.sqlite3-shm").exists());
    assert_eq!(std::fs::read(database).unwrap(), original);
}

#[cfg(target_os = "linux")]
#[test]
fn read_only_history_includes_committed_wal_records_without_changing_bytes() {
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        rustix::process::geteuid().as_raw(),
        0,
        "run this permissions fixture as an unprivileged user"
    );
    let root = tempfile::tempdir().unwrap();
    let binary = std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into());
    let scan = || {
        Command::new(&binary)
            .env_clear()
            .current_dir(root.path())
            .args(["fsck", "--root"])
            .arg(root.path())
            .output()
            .unwrap()
    };
    let bootstrap = scan();
    assert!(bootstrap.status.success(), "{bootstrap:?}");
    let database = root.path().join("metadata.sqlite3");
    // Retain the live database connection while a second command commits only
    // to WAL. An immutable/main-file-only reader would miss its new history.
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch("SELECT count(*) FROM sqlite_master")
        .unwrap();
    let original_database = std::fs::read(&database).unwrap();
    let fresh = scan();
    assert!(fresh.status.success(), "{fresh:?}");
    assert_eq!(std::fs::read(&database).unwrap(), original_database);
    let wal = root.path().join("metadata.sqlite3-wal");
    let shm = root.path().join("metadata.sqlite3-shm");
    let original_wal = std::fs::read(&wal).unwrap();
    assert!(!original_wal.is_empty());
    struct Restore(std::path::PathBuf, Vec<std::path::PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _root_restored =
                std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            for file in &self.1 {
                let _file_restored =
                    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644));
            }
        }
    }
    let files = vec![database.clone(), wal.clone(), shm];
    let _restore = Restore(root.path().to_path_buf(), files.clone());
    for file in files {
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o444)).unwrap();
    }
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let output = Command::new(&binary)
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let page: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(page["tasks"].as_array().unwrap().len(), 2);
    assert!(
        page["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|task| task["state"] == "completed")
    );
    assert_eq!(std::fs::read(database).unwrap(), original_database);
    assert_eq!(std::fs::read(wal).unwrap(), original_wal);
}
