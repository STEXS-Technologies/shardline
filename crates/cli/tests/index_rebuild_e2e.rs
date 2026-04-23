mod support;

use std::{
    env::var, error::Error, num::NonZeroUsize, path::PathBuf, process::Command, time::Duration,
};

use bytes::Bytes;
use rusqlite::Connection;
use shardline_server::LocalBackend;
use support::CliE2eInvariantError;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_restores_missing_latest_record() {
    let result = exercise_index_rebuild().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "index rebuild e2e failed: {error:?}");
}

async fn exercise_index_rebuild() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    sleep(Duration::from_millis(10)).await;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaaZZZZcccc"), None)
        .await?;

    let connection = Connection::open(storage.path().join("metadata.sqlite3"))?;
    connection.execute(
        "DELETE FROM shardline_file_records
         WHERE record_kind = 'latest' AND file_id = ?1",
        ["asset.bin"],
    )?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "index",
            "rebuild",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "index rebuild failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("rebuilt_latest_records: 1") {
        return Err(CliE2eInvariantError::new(
            "index rebuild did not report rebuilt latest records",
        )
        .into());
    }
    if !stdout.contains("issue_count: 0") {
        return Err(CliE2eInvariantError::new("index rebuild did not report zero issues").into());
    }

    let latest = backend.download_file("asset.bin", None, None).await?;
    if latest != b"aaaaZZZZcccc" {
        return Err(CliE2eInvariantError::new("index rebuild did not restore latest file").into());
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
