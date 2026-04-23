mod support;

use std::{
    env::var,
    error::Error,
    fs::{create_dir_all, read, write as write_file},
    path::{Path, PathBuf},
    process::Command,
};

use rusqlite::Connection;
use serde_json::{Value, from_slice};
use shardline_index::{IndexStore, LocalIndexStore, WebhookDelivery};
use shardline_protocol::RepositoryProvider;
use support::CliE2eInvariantError;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_deletes_zero_retention_candidates() {
    let result = exercise_gc_mark_and_sweep().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "gc e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_can_export_retention_report_and_orphan_inventory() {
    let result = exercise_gc_exports().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "gc export e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_deletes_orphan_native_xet_objects() {
    let result = exercise_gc_mark_and_sweep_for_native_xet_objects().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "gc native xet e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_fails_closed_on_corrupt_webhook_delivery_metadata() {
    let result = exercise_gc_fails_closed_on_corrupt_webhook_delivery_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc corrupt webhook metadata e2e failed: {error:?}"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_rejects_symlinked_export_artifact_path() {
    let result = exercise_gc_rejects_symlinked_export_artifact_path().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc symlinked artifact e2e failed: {error:?}"
    );
}

async fn exercise_gc_mark_and_sweep() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan")?;
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "gc",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--mark",
            "--sweep",
            "--retention-seconds",
            "0",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("mode: mark-and-sweep") {
        return Err(CliE2eInvariantError::new("gc did not report mark-and-sweep mode").into());
    }
    if !stdout.contains("retention_seconds: 0") {
        return Err(CliE2eInvariantError::new("gc did not report zero retention").into());
    }
    if !stdout.contains("new_quarantine_candidates: 1") {
        return Err(CliE2eInvariantError::new("gc did not quarantine the orphan chunk").into());
    }
    if !stdout.contains("deleted_chunks: 1") {
        return Err(CliE2eInvariantError::new("gc did not delete the orphan chunk").into());
    }
    if !stdout.contains("active_quarantine_candidates: 0") {
        return Err(CliE2eInvariantError::new("gc did not clear active quarantine state").into());
    }

    if orphan_path.exists() {
        return Err(CliE2eInvariantError::new("gc did not remove the orphan chunk file").into());
    }

    Ok(())
}

async fn exercise_gc_exports() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan")?;
    let retention_report_path = storage.path().join("artifacts").join("retention.json");
    let orphan_inventory_path = storage.path().join("artifacts").join("orphans.json");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "gc",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--mark",
            "--retention-seconds",
            "60",
            "--retention-report",
            retention_report_path
                .to_str()
                .ok_or("retention report path was not valid utf-8")?,
            "--orphan-inventory",
            orphan_inventory_path
                .to_str()
                .ok_or("orphan inventory path was not valid utf-8")?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc export failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("retention_report:") {
        return Err(CliE2eInvariantError::new("gc did not print retention report path").into());
    }
    if !stdout.contains("orphan_inventory:") {
        return Err(CliE2eInvariantError::new("gc did not print orphan inventory path").into());
    }

    let retention_report = from_slice::<Value>(&read(&retention_report_path)?)?;
    let orphan_inventory = from_slice::<Value>(&read(&orphan_inventory_path)?)?;

    let retention_entries = retention_report
        .as_array()
        .ok_or_else(|| CliE2eInvariantError::new("retention report was not an array"))?;
    let orphan_entries = orphan_inventory
        .as_array()
        .ok_or_else(|| CliE2eInvariantError::new("orphan inventory was not an array"))?;
    if retention_entries.len() != 1 {
        return Err(CliE2eInvariantError::new("retention report did not contain one entry").into());
    }
    if orphan_entries.len() != 1 {
        return Err(CliE2eInvariantError::new("orphan inventory did not contain one entry").into());
    }
    let retention_entry = retention_entries
        .first()
        .ok_or_else(|| CliE2eInvariantError::new("missing retention entry"))?;
    let orphan_entry = orphan_entries
        .first()
        .ok_or_else(|| CliE2eInvariantError::new("missing orphan entry"))?;

    if retention_entry.get("hash").and_then(Value::as_str) != Some(orphan_hash.as_str()) {
        return Err(CliE2eInvariantError::new("retention report hash was wrong").into());
    }
    if orphan_entry.get("hash").and_then(Value::as_str) != Some(orphan_hash.as_str()) {
        return Err(CliE2eInvariantError::new("orphan inventory hash was wrong").into());
    }
    if orphan_entry.get("quarantine_state").and_then(Value::as_str) != Some("quarantined") {
        return Err(
            CliE2eInvariantError::new("orphan inventory quarantine state was wrong").into(),
        );
    }

    Ok(())
}

async fn exercise_gc_mark_and_sweep_for_native_xet_objects() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let xorb_hash = "ab".repeat(32);
    let shard_hash = "cd".repeat(32);
    let xorb_path = write_orphan_object(
        storage.path(),
        &format!("xorbs/default/ab/{xorb_hash}.xorb"),
        b"serialized native xorb",
    )?;
    let shard_path = write_orphan_object(
        storage.path(),
        &format!("shards/cd/{shard_hash}.shard"),
        b"serialized retained shard",
    )?;
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "gc",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--mark",
            "--sweep",
            "--retention-seconds",
            "0",
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc native xet failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("new_quarantine_candidates: 2") {
        return Err(
            CliE2eInvariantError::new("gc did not quarantine orphan native xet objects").into(),
        );
    }
    if !stdout.contains("deleted_chunks: 2") {
        return Err(
            CliE2eInvariantError::new("gc did not delete orphan native xet objects").into(),
        );
    }

    if xorb_path.exists() {
        return Err(CliE2eInvariantError::new("gc did not remove the orphan xorb object").into());
    }
    if shard_path.exists() {
        return Err(CliE2eInvariantError::new("gc did not remove the orphan shard object").into());
    }

    Ok(())
}

#[cfg(unix)]
async fn exercise_gc_rejects_symlinked_export_artifact_path() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan")?;
    let target = storage.path().join("victim.json");
    write_file(&target, b"keep")?;
    let retention_report_path = storage.path().join("retention.json");
    symlink(&target, &retention_report_path)?;
    let orphan_inventory_path = storage.path().join("orphans.json");
    let shardline_bin = shardline_binary()?;

    let output = Command::new(shardline_bin)
        .args([
            "gc",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--mark",
            "--retention-seconds",
            "60",
            "--retention-report",
            retention_report_path
                .to_str()
                .ok_or("retention report path was not valid utf-8")?,
            "--orphan-inventory",
            orphan_inventory_path
                .to_str()
                .ok_or("orphan inventory path was not valid utf-8")?,
        ])
        .output()?;
    if output.status.success() {
        return Err(CliE2eInvariantError::new(
            "gc unexpectedly accepted a symlinked artifact path",
        )
        .into());
    }

    if read(&target)? != b"keep" {
        return Err(CliE2eInvariantError::new("gc changed the symlink target").into());
    }

    Ok(())
}

async fn exercise_gc_fails_closed_on_corrupt_webhook_delivery_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-corrupt".to_owned(),
        100,
    )?;
    assert!(index_store.record_webhook_delivery(&delivery)?);
    let connection = Connection::open(storage.path().join("metadata.sqlite3"))?;
    connection.execute(
        "UPDATE shardline_webhook_deliveries
         SET provider = 'invalid-provider'
         WHERE provider = 'github' AND owner = ?1 AND repo = ?2 AND delivery_id = ?3",
        [delivery.owner(), delivery.repo(), delivery.delivery_id()],
    )?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "gc",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--mark",
            "--sweep",
            "--retention-seconds",
            "0",
        ])
        .output()?;

    if output.status.success() {
        return Err(
            CliE2eInvariantError::new("gc unexpectedly succeeded on corrupt metadata").into(),
        );
    }
    if output.status.code() != Some(2) {
        return Err(CliE2eInvariantError::new("gc did not return operational failure code").into());
    }
    let stderr = String::from_utf8(output.stderr)?;
    if !stderr.contains("index adapter operation failed") {
        return Err(
            CliE2eInvariantError::new("gc did not fail through index adapter error").into(),
        );
    }
    let stored_provider = connection.query_row(
        "SELECT provider
         FROM shardline_webhook_deliveries
         WHERE owner = ?1 AND repo = ?2 AND delivery_id = ?3",
        [delivery.owner(), delivery.repo(), delivery.delivery_id()],
        |row| row.get::<_, String>(0),
    )?;
    if stored_provider != "invalid-provider" {
        return Err(CliE2eInvariantError::new("gc mutated corrupt metadata after failing").into());
    }

    Ok(())
}

fn write_orphan_chunk(root: &Path, hash: &str, bytes: &[u8]) -> Result<PathBuf, Box<dyn Error>> {
    let path = root.join("chunks").join(&hash[..2]).join(hash);
    let Some(parent) = path.parent() else {
        return Err(CliE2eInvariantError::new("orphan chunk parent directory missing").into());
    };
    create_dir_all(parent)?;
    write_file(&path, bytes)?;
    Ok(path)
}

fn write_orphan_object(
    root: &Path,
    object_key: &str,
    bytes: &[u8],
) -> Result<PathBuf, Box<dyn Error>> {
    let path = root.join("chunks").join(object_key);
    let Some(parent) = path.parent() else {
        return Err(CliE2eInvariantError::new("orphan object parent directory missing").into());
    };
    create_dir_all(parent)?;
    write_file(&path, bytes)?;
    Ok(path)
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
