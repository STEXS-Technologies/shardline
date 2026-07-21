mod support;

use std::{env::var, error::Error, path::PathBuf, process::Command};

use support::CliE2eInvariantError;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hold_set_list_and_release_roundtrip() {
    let result = exercise_hold_roundtrip().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "hold e2e failed: {error:?}");
}

async fn exercise_hold_roundtrip() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let shardline_bin = shardline_binary()?;
    let object_key = format!("de/{}", "de".repeat(32));

    let set_output = Command::new(&shardline_bin)
        .args([
            "hold",
            "set",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--object-key",
            &object_key,
            "--reason",
            "provider deletion grace",
            "--ttl-seconds",
            "600",
        ])
        .output()?;
    if !set_output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "hold set failed: {}",
            String::from_utf8_lossy(&set_output.stderr)
        ))
        .into());
    }
    let set_stdout = String::from_utf8(set_output.stdout)?;
    if !set_stdout.contains("object_key: ") || !set_stdout.contains(object_key.as_str()) {
        return Err(CliE2eInvariantError::new("hold set did not print the object key").into());
    }

    let list_output = Command::new(&shardline_bin)
        .args([
            "hold",
            "list",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--active-only",
        ])
        .output()?;
    if !list_output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "hold list failed: {}",
            String::from_utf8_lossy(&list_output.stderr)
        ))
        .into());
    }
    let list_stdout = String::from_utf8(list_output.stdout)?;
    if !list_stdout.contains("hold_count: 1") {
        return Err(CliE2eInvariantError::new("hold list did not report one hold").into());
    }
    if !list_stdout.contains(object_key.as_str()) {
        return Err(CliE2eInvariantError::new("hold list did not include the object key").into());
    }

    let release_output = Command::new(&shardline_bin)
        .args([
            "hold",
            "release",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--object-key",
            &object_key,
        ])
        .output()?;
    if !release_output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "hold release failed: {}",
            String::from_utf8_lossy(&release_output.stderr)
        ))
        .into());
    }
    let release_stdout = String::from_utf8(release_output.stdout)?;
    if !release_stdout.contains("released: true") {
        return Err(CliE2eInvariantError::new("hold release did not report success").into());
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
