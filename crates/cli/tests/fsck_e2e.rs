mod support;

use std::{
    env::var,
    error::Error,
    fs::{remove_file, write as write_file},
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
};

use bytes::Bytes;
use rusqlite::Connection;
use shardline_index::{IndexStore, LocalIndexStore, WebhookDelivery};
use shardline_protocol::RepositoryProvider;
use shardline_server::LocalBackend;
use support::CliE2eInvariantError;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_succeeds_for_clean_storage_and_fails_for_missing_chunk() {
    let result = exercise_fsck().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "fsck e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_detects_corrupted_reachable_chunk_after_gc_leaves_it_in_place() {
    let result = exercise_corrupted_reachable_chunk_is_detected_after_gc().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "corrupted reachable chunk fsck/gc e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_fails_closed_on_corrupt_webhook_delivery_metadata() {
    let result = exercise_fsck_fails_closed_on_corrupt_webhook_delivery_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "corrupt webhook metadata fsck e2e failed: {error:?}"
    );
}

async fn exercise_fsck() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    let shardline_bin = shardline_binary()?;

    let clean_output = Command::new(&shardline_bin)
        .args([
            "fsck",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
        ])
        .output()?;
    if !clean_output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "clean fsck failed: {}",
            String::from_utf8_lossy(&clean_output.stderr)
        ))
        .into());
    }
    let clean_stdout = String::from_utf8(clean_output.stdout)?;
    if !clean_stdout.contains("issue_count: 0") {
        return Err(CliE2eInvariantError::new("clean fsck did not report zero issues").into());
    }

    let first_chunk = uploaded.chunks.first().ok_or_else(|| {
        CliE2eInvariantError::new("uploaded file did not produce any chunk records")
    })?;
    let hash_prefix = first_chunk
        .hash
        .get(..2)
        .ok_or_else(|| CliE2eInvariantError::new("chunk hash prefix was missing"))?;
    let chunk_path = storage
        .path()
        .join("chunks")
        .join(hash_prefix)
        .join(&first_chunk.hash);
    remove_file(chunk_path)?;

    let broken_output = Command::new(shardline_bin)
        .args([
            "fsck",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
        ])
        .output()?;
    if broken_output.status.success() {
        return Err(CliE2eInvariantError::new("broken fsck unexpectedly succeeded").into());
    }
    let broken_stdout = String::from_utf8(broken_output.stdout)?;
    let broken_stderr = String::from_utf8(broken_output.stderr)?;
    if !broken_stdout.contains("issue_count: 2") {
        return Err(CliE2eInvariantError::new("broken fsck did not report issue count").into());
    }
    if !broken_stderr.contains("issue: missing_chunk") {
        return Err(CliE2eInvariantError::new("broken fsck did not report missing chunk").into());
    }

    Ok(())
}

async fn exercise_corrupted_reachable_chunk_is_detected_after_gc() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    let first_chunk = uploaded.chunks.first().ok_or_else(|| {
        CliE2eInvariantError::new("uploaded file did not produce any chunk records")
    })?;
    let hash_prefix = first_chunk
        .hash
        .get(..2)
        .ok_or_else(|| CliE2eInvariantError::new("chunk hash prefix was missing"))?;
    let chunk_path = storage
        .path()
        .join("chunks")
        .join(hash_prefix)
        .join(&first_chunk.hash);
    let corrupt_len = usize::try_from(first_chunk.length)?;
    write_file(&chunk_path, vec![0x5e; corrupt_len])?;

    let shardline_bin = shardline_binary()?;
    let gc_output = Command::new(&shardline_bin)
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
    if !gc_output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "gc failed for corrupted reachable chunk: {}",
            String::from_utf8_lossy(&gc_output.stderr)
        ))
        .into());
    }
    let gc_stdout = String::from_utf8(gc_output.stdout)?;
    if !gc_stdout.contains("orphan_chunks: 0") {
        return Err(CliE2eInvariantError::new(
            "gc treated a corrupted reachable chunk as orphaned",
        )
        .into());
    }
    if !gc_stdout.contains("deleted_chunks: 0") {
        return Err(CliE2eInvariantError::new("gc deleted a corrupted but reachable chunk").into());
    }
    if !chunk_path.exists() {
        return Err(CliE2eInvariantError::new("gc removed the corrupted reachable chunk").into());
    }

    let fsck_output = Command::new(shardline_bin)
        .args([
            "fsck",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
        ])
        .output()?;
    if fsck_output.status.success() {
        return Err(CliE2eInvariantError::new("corrupt fsck unexpectedly succeeded").into());
    }
    let fsck_stderr = String::from_utf8(fsck_output.stderr)?;
    if !fsck_stderr.contains("issue: chunk_hash_mismatch") {
        return Err(CliE2eInvariantError::new("fsck did not report chunk hash mismatch").into());
    }

    Ok(())
}

async fn exercise_fsck_fails_closed_on_corrupt_webhook_delivery_metadata()
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
            "fsck",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
        ])
        .output()?;

    if output.status.success() {
        return Err(
            CliE2eInvariantError::new("fsck unexpectedly succeeded on corrupt metadata").into(),
        );
    }
    if output.status.code() != Some(2) {
        return Err(
            CliE2eInvariantError::new("fsck did not return operational failure code").into(),
        );
    }
    let stderr = String::from_utf8(output.stderr)?;
    if !stderr.contains("index adapter operation failed") {
        return Err(
            CliE2eInvariantError::new("fsck did not fail through index adapter error").into(),
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
        return Err(
            CliE2eInvariantError::new("fsck mutated corrupt metadata after failing").into(),
        );
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
