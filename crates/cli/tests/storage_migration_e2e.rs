mod support;

use std::{env::var, error::Error, num::NonZeroUsize, path::PathBuf, process::Command};

use bytes::Bytes;
use shardline_server::LocalBackend;
use support::CliE2eInvariantError;

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
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;

    let shardline_bin = shardline_binary()?;
    let first = Command::new(&shardline_bin)
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
        ])
        .output()?;
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

    let second = Command::new(shardline_bin)
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
        ])
        .output()?;
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
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    let root_link = source.path().join("root-link");
    symlink(redirected.path(), &root_link)?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
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
        ])
        .output()?;
    if output.status.success() {
        return Err(CliE2eInvariantError::new(
            "storage migration unexpectedly accepted a symlinked local root",
        )
        .into());
    }

    Ok(())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace_root) = manifest_dir.ancestors().nth(2) else {
        return Err(CliE2eInvariantError::new("workspace root could not be resolved").into());
    };
    let binary = workspace_root
        .join("target")
        .join("debug")
        .join("shardline");
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }

    Ok(binary)
}
