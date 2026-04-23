mod support;

use std::{
    env::var,
    error::Error,
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rusqlite::Connection;
use shardline_index::{
    IndexStore, LocalIndexStore, QuarantineCandidate, RetentionHold, WebhookDelivery,
};
use shardline_protocol::RepositoryProvider;
use shardline_server::{DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, LocalBackend};
use shardline_storage::ObjectKey;
use support::CliE2eInvariantError;
use tokio::{
    fs::{create_dir_all, write as write_file},
    time::sleep,
};

// This CLI e2e launches a separate process, so the test fixture clock cannot be pinned to the
// repair engine's runtime clock. Exact boundary semantics are covered deterministically in
// `shardline_server::lifecycle_repair::tests`.
const WEBHOOK_REPAIR_TIME_MARGIN_SECONDS: u64 = 120;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_cleans_stale_metadata_and_keeps_live_quarantine() {
    let result = exercise_lifecycle_repair().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "lifecycle repair e2e failed: {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_fails_closed_on_corrupt_webhook_delivery_metadata() {
    let result =
        exercise_lifecycle_repair_fails_closed_on_corrupt_webhook_delivery_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "lifecycle repair corrupt metadata e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_orchestrator_rebuilds_repairs_and_verifies() {
    let result = exercise_repair_orchestrator().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "repair orchestrator e2e failed: {error:?}");
}

async fn exercise_lifecycle_repair() -> Result<(), Box<dyn Error>> {
    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
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
    let first_chunk = uploaded
        .chunks
        .first()
        .ok_or_else(|| CliE2eInvariantError::new("missing uploaded chunk"))?;
    let live_object_key =
        ObjectKey::parse(&format!("{}/{}", &first_chunk.hash[..2], first_chunk.hash))?;
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        live_object_key.clone(),
        first_chunk.length,
        10,
        20,
    )?)?;

    let missing_candidate_key = ObjectKey::parse(&format!("ff/{}", "ff".repeat(32)))?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        missing_candidate_key,
        12,
        11,
        21,
    )?)?;

    let missing_hold_key = ObjectKey::parse(&format!("de/{}", "de".repeat(32)))?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        missing_hold_key,
        "missing object".to_owned(),
        10,
        Some(20),
    )?)?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        live_object_key,
        "active hold".to_owned(),
        10,
        Some(i64::MAX as u64),
    )?)?;

    let orphan_key = ObjectKey::parse(&format!("ef/{}", "ef".repeat(32)))?;
    let orphan_path = storage
        .path()
        .join("chunks")
        .join("ef")
        .join("ef".repeat(32));
    let parent = orphan_path
        .parent()
        .ok_or_else(|| CliE2eInvariantError::new("orphan chunk path missing parent"))?;
    create_dir_all(parent).await?;
    write_file(&orphan_path, b"orphan").await?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(orphan_key, 6, 50, 60)?)?;

    let held_orphan_key = ObjectKey::parse(&format!("aa/{}", "aa".repeat(32)))?;
    let held_orphan_path = storage
        .path()
        .join("chunks")
        .join("aa")
        .join("aa".repeat(32));
    let held_parent = held_orphan_path
        .parent()
        .ok_or_else(|| CliE2eInvariantError::new("held orphan path missing parent"))?;
    create_dir_all(held_parent).await?;
    write_file(&held_orphan_path, b"held-orphan").await?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        held_orphan_key.clone(),
        "manual retention".to_owned(),
        10,
        Some(i64::MAX as u64),
    )?)?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        held_orphan_key,
        11,
        70,
        80,
    )?)?;

    let old_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-old".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
                .saturating_add(WEBHOOK_REPAIR_TIME_MARGIN_SECONDS),
        ),
    )?;
    let future_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-future".to_owned(),
        now_unix_seconds.saturating_add(WEBHOOK_REPAIR_TIME_MARGIN_SECONDS + 300),
    )?;
    let fresh_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-fresh".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
                .saturating_sub(WEBHOOK_REPAIR_TIME_MARGIN_SECONDS),
        ),
    )?;
    let _recorded_old = index_store.record_webhook_delivery(&old_delivery)?;
    let _recorded_future = index_store.record_webhook_delivery(&future_delivery)?;
    let _recorded_fresh = index_store.record_webhook_delivery(&fresh_delivery)?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "repair",
            "lifecycle",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--webhook-retention-seconds",
            &DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "repair lifecycle failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("scanned_quarantine_candidates: 4") {
        return Err(
            CliE2eInvariantError::new("repair did not scan expected quarantine count").into(),
        );
    }
    if !stdout.contains("removed_missing_quarantine_candidates: 1") {
        return Err(CliE2eInvariantError::new(
            "repair did not remove missing quarantine candidate",
        )
        .into());
    }
    if !stdout.contains("removed_reachable_quarantine_candidates: 1") {
        return Err(CliE2eInvariantError::new(
            "repair did not remove reachable quarantine candidate",
        )
        .into());
    }
    if !stdout.contains("removed_held_quarantine_candidates: 1") {
        return Err(
            CliE2eInvariantError::new("repair did not remove held quarantine candidate").into(),
        );
    }
    if !stdout.contains("removed_expired_retention_holds: 1") {
        return Err(
            CliE2eInvariantError::new("repair did not remove expired retention hold").into(),
        );
    }
    if !stdout.contains("removed_stale_webhook_deliveries: 1") {
        return Err(
            CliE2eInvariantError::new("repair did not remove stale webhook delivery").into(),
        );
    }
    if !stdout.contains("removed_future_webhook_deliveries: 1") {
        return Err(
            CliE2eInvariantError::new("repair did not remove future webhook delivery").into(),
        );
    }

    let remaining_holds = index_store.list_retention_holds()?;
    if remaining_holds.len() != 2 {
        return Err(CliE2eInvariantError::new("repair left unexpected hold count").into());
    }
    let remaining_quarantine = index_store.list_quarantine_candidates()?;
    if remaining_quarantine.len() != 1 {
        return Err(CliE2eInvariantError::new("repair left unexpected quarantine count").into());
    }
    let remaining_deliveries = index_store.list_webhook_deliveries()?;
    if remaining_deliveries != vec![fresh_delivery] {
        return Err(CliE2eInvariantError::new("repair left unexpected webhook deliveries").into());
    }

    Ok(())
}

async fn exercise_repair_orchestrator() -> Result<(), Box<dyn Error>> {
    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
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

    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let old_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-old".to_owned(),
        now_unix_seconds.saturating_sub(DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS),
    )?;
    let _recorded = index_store.record_webhook_delivery(&old_delivery)?;

    let shardline_bin = shardline_binary()?;
    let output = Command::new(shardline_bin)
        .args([
            "repair",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--webhook-retention-seconds",
            &DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "repair failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;
    if !stdout.contains("index_rebuild.rebuilt_latest_records: 1") {
        return Err(CliE2eInvariantError::new("repair did not rebuild latest records").into());
    }
    if !stdout.contains("lifecycle_repair.removed_stale_webhook_deliveries: 1") {
        return Err(
            CliE2eInvariantError::new("repair did not prune stale webhook delivery").into(),
        );
    }
    if !stdout.contains("fsck.issue_count: 0") {
        return Err(CliE2eInvariantError::new("repair did not finish with clean fsck").into());
    }

    let latest = backend.download_file("asset.bin", None, None).await?;
    if latest != b"aaaaZZZZcccc" {
        return Err(CliE2eInvariantError::new("repair did not restore latest file").into());
    }
    if !index_store.list_webhook_deliveries()?.is_empty() {
        return Err(CliE2eInvariantError::new("repair left stale webhook delivery").into());
    }

    Ok(())
}

async fn exercise_lifecycle_repair_fails_closed_on_corrupt_webhook_delivery_metadata()
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
            "repair",
            "lifecycle",
            "--root",
            storage
                .path()
                .to_str()
                .ok_or("storage path was not valid utf-8")?,
            "--webhook-retention-seconds",
            &DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS.to_string(),
        ])
        .output()?;

    if output.status.success() {
        return Err(
            CliE2eInvariantError::new("repair unexpectedly succeeded on corrupt metadata").into(),
        );
    }
    if output.status.code() != Some(2) {
        return Err(
            CliE2eInvariantError::new("repair did not return operational failure code").into(),
        );
    }
    let stderr = String::from_utf8(output.stderr)?;
    if !stderr.contains("index adapter operation failed") {
        return Err(
            CliE2eInvariantError::new("repair did not fail through index adapter error").into(),
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
            CliE2eInvariantError::new("repair mutated corrupt metadata after failing").into(),
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
