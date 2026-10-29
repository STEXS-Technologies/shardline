mod support;

use std::{
    env::var,
    error::Error,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::Duration,
};

use bytes::Bytes;
use shardline_server::LocalBackend;
use support::CliE2eInvariantError;

fn run(command: &mut Command) -> std::io::Result<Output> {
    shardline_test_support::native_client_output(command, Duration::from_secs(120))
}

fn verify_packed_payloads(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let relative = Path::new("chunks/xorbs/default");
    let mut verified = false;
    for prefix in std::fs::read_dir(source.join(relative))? {
        for object in std::fs::read_dir(prefix?.path())? {
            let path = object?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "xorb")
            {
                let key = path.strip_prefix(source)?;
                if std::fs::read(&path)? != std::fs::read(destination.join(key))? {
                    return Err(CliE2eInvariantError::new(
                        "migration changed packed payload bytes",
                    )
                    .into());
                }
                verified = true;
            }
        }
    }
    if !verified {
        return Err(
            CliE2eInvariantError::new("migration must verify a persisted packed payload").into(),
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_migrate_copies_local_objects_idempotently() {
    let result = exercise_storage_migrate_local_to_local().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "storage migration e2e failed: {error:?}");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_migrate_rejects_symlinked_local_root() {
    let result = exercise_storage_migrate_rejects_symlinked_local_root().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "storage migration symlink root e2e failed: {error:?}"
    );
}

async fn exercise_storage_migrate_local_to_local() -> Result<(), Box<dyn Error>> {
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        source.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;

    let shardline_bin = shardline_binary()?;
    let first = run(Command::new(&shardline_bin)
        .env_remove("SHARDLINE_ROOT_DIR")
        .args([
            "storage",
            "migrate",
            "--from",
            "local",
            "--from-root",
            source
                .path()
                .to_str()
                .ok_or("source path was not valid utf-8")?,
            "--to",
            "local",
            "--to-root",
            destination
                .path()
                .to_str()
                .ok_or("destination path was not valid utf-8")?,
        ]))?;
    if !first.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "first migration failed: {}",
            String::from_utf8_lossy(&first.stderr)
        ))
        .into());
    }
    let first_stdout = String::from_utf8(first.stdout)?;
    if !first_stdout.contains("inserted_objects: ") {
        return Err(CliE2eInvariantError::new("migration did not report inserted objects").into());
    }
    if first_stdout.contains("inserted_objects: 0") {
        return Err(CliE2eInvariantError::new("migration did not copy any objects").into());
    }

    verify_packed_payloads(source.path(), destination.path())?;

    let second = run(Command::new(shardline_bin)
        .env_remove("SHARDLINE_ROOT_DIR")
        .args([
            "storage",
            "migrate",
            "--from",
            "local",
            "--from-root",
            source
                .path()
                .to_str()
                .ok_or("source path was not valid utf-8")?,
            "--to",
            "local",
            "--to-root",
            destination
                .path()
                .to_str()
                .ok_or("destination path was not valid utf-8")?,
        ]))?;
    if !second.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "second migration failed: {}",
            String::from_utf8_lossy(&second.stderr)
        ))
        .into());
    }
    let second_stdout = String::from_utf8(second.stdout)?;
    if !second_stdout.contains("inserted_objects: 0") {
        return Err(CliE2eInvariantError::new("second migration was not idempotent").into());
    }
    if !second_stdout.contains("already_present_objects: ") {
        return Err(
            CliE2eInvariantError::new("second migration did not report existing objects").into(),
        );
    }

    verify_packed_payloads(source.path(), destination.path())?;
    Ok(())
}

#[cfg(unix)]
async fn exercise_storage_migrate_rejects_symlinked_local_root() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let source = tempfile::tempdir()?;
    let redirected = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        redirected.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    let root_link = source.path().join("root-link");
    symlink(redirected.path(), &root_link)?;

    let shardline_bin = shardline_binary()?;
    let output = run(Command::new(shardline_bin)
        .env_remove("SHARDLINE_ROOT_DIR")
        .args([
            "storage",
            "migrate",
            "--from",
            "local",
            "--from-root",
            root_link
                .to_str()
                .ok_or("source path was not valid utf-8")?,
            "--to",
            "local",
            "--to-root",
            destination
                .path()
                .to_str()
                .ok_or("destination path was not valid utf-8")?,
        ]))?;
    if output.status.success() {
        return Err(CliE2eInvariantError::new(
            "storage migration unexpectedly accepted a symlinked local root",
        )
        .into());
    }

    Ok(())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    let binary = var("CARGO_BIN_EXE_shardline")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_BIN_EXE_shardline")));
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }

    Ok(binary)
}
