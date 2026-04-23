mod support;

use std::{
    env::var,
    error::Error,
    fs::{read, write as write_file},
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
};

use bytes::Bytes;
use serde_json::{Value, from_slice};
use shardline_server::LocalBackend;
use support::CliE2eInvariantError;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_manifest_inventories_local_deployment() {
    let result = exercise_backup_manifest().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "backup manifest e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_manifest_uses_current_directory_deployment_root() {
    let result = exercise_backup_manifest_from_current_directory().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "backup manifest cwd root e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_manifest_uses_project_local_state_root() {
    let result = exercise_backup_manifest_from_project_directory().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "backup manifest project root e2e failed: {error:?}"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_manifest_rejects_symlinked_output_path() {
    let result = exercise_backup_manifest_rejects_symlinked_output_path().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "backup manifest symlink output e2e failed: {error:?}"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_manifest_rejects_symlinked_root_override() {
    let result = exercise_backup_manifest_rejects_symlinked_root_override().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "backup manifest symlink root e2e failed: {error:?}"
    );
}

async fn exercise_backup_manifest() -> Result<(), Box<dyn Error>> {
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

    let output_path = storage.path().join("backup-manifest.json");
    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "backup",
            "manifest",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--output",
            output_path
                .to_str()
                .ok_or("output path was not valid utf-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "backup manifest failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("metadata_backend: local") {
        return Err(
            CliE2eInvariantError::new("backup did not report local metadata backend").into(),
        );
    }
    if !stdout.contains("object_backend: local") {
        return Err(CliE2eInvariantError::new("backup did not report local object backend").into());
    }
    if !stdout.contains("latest_records: 1") {
        return Err(CliE2eInvariantError::new("backup did not count latest records").into());
    }
    if !stdout.contains("version_records: 1") {
        return Err(CliE2eInvariantError::new("backup did not count version records").into());
    }

    let manifest: Value = from_slice(&read(output_path)?)?;
    if manifest.get("manifest_version").and_then(Value::as_u64) != Some(1) {
        return Err(CliE2eInvariantError::new("manifest version mismatch").into());
    }
    if manifest.get("metadata_backend").and_then(Value::as_str) != Some("local") {
        return Err(CliE2eInvariantError::new("manifest metadata backend mismatch").into());
    }
    if manifest.get("object_backend").and_then(Value::as_str) != Some("local") {
        return Err(CliE2eInvariantError::new("manifest object backend mismatch").into());
    }
    if manifest.get("latest_records").and_then(Value::as_u64) != Some(1) {
        return Err(CliE2eInvariantError::new("manifest latest count mismatch").into());
    }
    if manifest.get("version_records").and_then(Value::as_u64) != Some(1) {
        return Err(CliE2eInvariantError::new("manifest version count mismatch").into());
    }

    let objects = manifest
        .get("objects")
        .and_then(Value::as_array)
        .ok_or_else(|| CliE2eInvariantError::new("manifest objects was not an array"))?;
    if objects.is_empty() {
        return Err(CliE2eInvariantError::new("manifest did not inventory objects").into());
    }
    if manifest.get("object_count").and_then(Value::as_u64) != Some(u64::try_from(objects.len())?) {
        return Err(CliE2eInvariantError::new("manifest object count mismatch").into());
    }

    Ok(())
}

async fn exercise_backup_manifest_from_current_directory() -> Result<(), Box<dyn Error>> {
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

    let output_path = storage.path().join("backup-manifest-cwd.json");
    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .current_dir(storage.path())
        .env_remove("SHARDLINE_ROOT_DIR")
        .args([
            "backup",
            "manifest",
            "--output",
            output_path
                .to_str()
                .ok_or("output path was not valid utf-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "backup manifest from cwd failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains(&format!("root: {}", storage.path().display())) {
        return Err(CliE2eInvariantError::new("backup did not resolve cwd as root").into());
    }
    let manifest: Value = from_slice(&read(output_path)?)?;
    if manifest.get("latest_records").and_then(Value::as_u64) != Some(1) {
        return Err(CliE2eInvariantError::new("manifest latest count mismatch").into());
    }

    Ok(())
}

async fn exercise_backup_manifest_from_project_directory() -> Result<(), Box<dyn Error>> {
    let project = tempfile::tempdir()?;
    let storage_root = project.path().join(".shardline").join("data");
    let backend = LocalBackend::new(
        storage_root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;

    let output_path = project.path().join("backup-manifest-project.json");
    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .current_dir(project.path())
        .env_remove("SHARDLINE_ROOT_DIR")
        .args([
            "backup",
            "manifest",
            "--output",
            output_path
                .to_str()
                .ok_or("output path was not valid utf-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "backup manifest from project directory failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains(&format!("root: {}", storage_root.display())) {
        return Err(
            CliE2eInvariantError::new("backup did not resolve project-local state root").into(),
        );
    }
    let manifest: Value = from_slice(&read(output_path)?)?;
    if manifest.get("latest_records").and_then(Value::as_u64) != Some(1) {
        return Err(CliE2eInvariantError::new("manifest latest count mismatch").into());
    }

    Ok(())
}

#[cfg(unix)]
async fn exercise_backup_manifest_rejects_symlinked_output_path() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

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

    let target = storage.path().join("victim.json");
    write_file(&target, b"keep")?;
    let output_path = storage.path().join("backup-manifest.json");
    symlink(&target, &output_path)?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "backup",
            "manifest",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--output",
            output_path
                .to_str()
                .ok_or("output path was not valid utf-8")?,
        ])
        .output()?;
    if output.status.success() {
        return Err(CliE2eInvariantError::new(
            "backup manifest unexpectedly accepted a symlinked output path",
        )
        .into());
    }

    if read(&target)? != b"keep" {
        return Err(CliE2eInvariantError::new("backup manifest changed the symlink target").into());
    }

    Ok(())
}

#[cfg(unix)]
async fn exercise_backup_manifest_rejects_symlinked_root_override() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let storage = tempfile::tempdir()?;
    let redirected = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        redirected.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;

    let root_link = storage.path().join("root-link");
    symlink(redirected.path(), &root_link)?;
    let output_path = storage.path().join("backup-manifest.json");
    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "backup",
            "manifest",
            "--root",
            root_link
                .to_str()
                .ok_or("symlink root path was not valid utf-8")?,
            "--output",
            output_path
                .to_str()
                .ok_or("output path was not valid utf-8")?,
        ])
        .output()?;
    if output.status.success() {
        return Err(CliE2eInvariantError::new(
            "backup manifest unexpectedly accepted a symlinked root override",
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
