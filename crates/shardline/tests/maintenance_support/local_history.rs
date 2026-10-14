use super::super::*;

#[test]
fn local_history_rejects_conflicting_modes_and_never_initializes_metadata() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("sentinel"), b"preserved")?;
    for mode in ["--archive", "--pending-deletes"] {
        let output = Command::new(binary())
            .env_clear()
            .args(["history", "--local", mode, "--root"])
            .arg(root.path())
            .output()?;
        assert!(!output.status.success());
    }
    let _output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--local", "--root"])
        .arg(root.path())
        .output()?;
    assert!(!root.path().join("metadata.sqlite3").exists());
    assert!(!root.path().join("chunks").exists());
    assert_eq!(fs::read(root.path().join("sentinel"))?, b"preserved");
    Ok(())
}

#[test]
fn local_copy_history_remains_inspectable_with_unrelated_postgres_settings() -> Result<()> {
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    fs::create_dir_all(source.path().join("chunks/copy"))?;
    fs::write(source.path().join("chunks/copy/first"), b"copied bytes")?;
    let unavailable = "postgres://unused:unused@127.0.0.1:1/unused";
    let copied = Command::new(binary())
        .env_clear()
        .current_dir(source.path())
        .env("SHARDLINE_INDEX_POSTGRES_URL", unavailable)
        .args(["storage", "migrate", "--from", "local", "--from-root"])
        .arg(source.path())
        .args(["--to", "local", "--to-root"])
        .arg(destination.path())
        .output()?;
    assert!(copied.status.success(), "{copied:?}");
    let inspect = |local| {
        let mut command = Command::new(binary());
        command
            .env_clear()
            .current_dir(source.path())
            .env("SHARDLINE_INDEX_POSTGRES_URL", unavailable)
            .arg("history");
        if local {
            command.arg("--local");
        }
        command.arg("--root").arg(destination.path()).output()
    };
    assert!(
        !inspect(false)?.status.success(),
        "normal history must retain its configured database scope"
    );
    fs::write(source.path().join("chunks/copy/later"), b"do not replay")?;
    let output = inspect(true)?;
    assert!(output.status.success(), "{output:?}");
    let page: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        page.get("history_source"),
        Some(&serde_json::json!("local_sqlite"))
    );
    assert_eq!(
        page.pointer("/tasks/0/state"),
        Some(&serde_json::json!("completed"))
    );
    assert_eq!(
        page.get("tasks")
            .and_then(serde_json::Value::as_array)
            .ok_or("missing tasks")?
            .len(),
        1
    );
    assert!(
        page.get("inspection_notice")
            .and_then(serde_json::Value::as_str)
            .ok_or("missing scope notice")?
            .contains("PostgreSQL history is not included")
    );
    assert_eq!(
        fs::read(destination.path().join("chunks/copy/first"))?,
        b"copied bytes"
    );
    assert!(!destination.path().join("chunks/copy/later").exists());
    Ok(())
}
