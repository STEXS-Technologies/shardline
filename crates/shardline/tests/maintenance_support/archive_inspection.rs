use super::super::*;
use std::os::unix::fs::PermissionsExt;

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
