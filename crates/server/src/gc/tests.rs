use std::{
    error::Error,
    fmt::Debug,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use axum::body::Bytes;
use rusqlite::{Connection, params};
use serde::Serialize;
use serde_json::to_vec;
use shardline_index::{
    DedupeShardMapping, IndexStore, LocalIndexStore, QuarantineCandidate, RetentionHold,
    WebhookDelivery,
};
use shardline_protocol::{RepositoryProvider, ShardlineHash, unix_now_seconds_lossy};
use shardline_storage::ObjectKey;
use tokio::fs;

use super::{
    GcOrphanQuarantineState, LocalGcOptions, quarantine_record_path, quarantine_root, run_local_gc,
    run_local_gc_diagnostics,
};
use crate::{
    LocalBackend, ServerError, ShardMetadataLimits,
    chunk_store::chunk_object_key,
    local_backend::chunk_hash,
    test_fixtures::{shard_hash_hex, single_chunk_xorb, single_file_shard},
    test_invariant_error::ServerTestInvariantError,
    upload_ingest::RequestBodyReader,
    xet_adapter::{shard_object_key, xorb_object_key},
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct QuarantineRecord {
    hash: String,
    bytes: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_dry_run_reports_orphan_chunks_without_mutating_quarantine() {
    let result = exercise_gc_dry_run_reports_orphan_chunks_without_mutating_quarantine().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc dry run reports orphan chunks without mutating quarantine failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_only_creates_quarantine_candidates() {
    let result = exercise_gc_mark_only_creates_quarantine_candidates().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark only creates quarantine candidates failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_only_reaches_a_fixed_point_on_second_run() {
    let result = exercise_gc_mark_only_reaches_a_fixed_point_on_second_run().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark-only fixed-point behavior failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_diagnostics_include_retention_and_orphan_state() {
    let result = exercise_gc_diagnostics_include_retention_and_orphan_state().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc diagnostics include retention and orphan state failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_sweep_only_deletes_expired_quarantine_candidates() {
    let result = exercise_gc_sweep_only_deletes_expired_quarantine_candidates().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc sweep only deletes expired quarantine candidates failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_releases_quarantine_candidates_when_chunk_becomes_reachable() {
    let result = exercise_gc_releases_quarantine_candidates_when_chunk_becomes_reachable().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc releases quarantine candidates when chunk becomes reachable failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_with_zero_retention_deletes_new_candidates() {
    let result = exercise_gc_mark_and_sweep_with_zero_retention_deletes_new_candidates().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark and sweep with zero retention deletes new candidates failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_active_retention_holds_exclude_chunks_from_orphans() {
    let result = exercise_gc_active_retention_holds_exclude_chunks_from_orphans().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc active retention holds exclude chunks from orphans failed: {error:?}"
    );
}

#[tokio::test]
async fn gc_dry_run_keeps_expired_retention_holds() {
    let result = exercise_gc_dry_run_keeps_expired_retention_holds().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc dry run expired retention hold contract failed: {error:?}"
    );
}

#[tokio::test]
async fn gc_mutating_run_prunes_expired_retention_holds() {
    let result = exercise_gc_mutating_run_prunes_expired_retention_holds().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mutating expired retention hold pruning failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_deletes_orphan_native_xorb_object() {
    let result = exercise_gc_mark_and_sweep_deletes_orphan_native_xorb_object().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark and sweep deletes orphan native xorb object failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_deletes_orphan_retained_shard_object() {
    let result = exercise_gc_mark_and_sweep_deletes_orphan_retained_shard_object().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark and sweep deletes orphan retained shard object failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_stale_dedupe_mapping_does_not_keep_retained_shard_alive() {
    let result = exercise_gc_stale_dedupe_mapping_does_not_keep_retained_shard_alive().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc stale dedupe mapping should not keep retained shard alive failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_live_native_xet_record_keeps_retained_shard_reachable() {
    let result = exercise_gc_live_native_xet_record_keeps_retained_shard_reachable().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc live native xet record should keep retained shard reachable failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_fails_closed_on_corrupt_webhook_delivery_metadata() {
    let result = exercise_gc_fails_closed_on_corrupt_webhook_delivery_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc should fail closed on corrupt webhook delivery metadata: {error:?}"
    );
}

async fn exercise_gc_dry_run_reports_orphan_chunks_without_mutating_quarantine()
-> Result<(), Box<dyn Error>> {
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

    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let report = run_local_gc(storage.path().to_path_buf(), LocalGcOptions::dry_run()).await?;

    ensure_eq(
        &report.scanned_records,
        &2,
        "unexpected scanned record count",
    )?;
    ensure_eq(
        &report.referenced_chunks,
        &3,
        "unexpected referenced chunk count",
    )?;
    ensure_eq(&report.orphan_chunks, &1, "unexpected orphan chunk count")?;
    ensure_eq(
        &report.orphan_chunk_bytes,
        &6,
        "unexpected orphan byte count",
    )?;
    ensure_eq(
        &report.active_quarantine_candidates,
        &0,
        "unexpected active quarantine candidate count",
    )?;
    ensure_eq(
        &report.new_quarantine_candidates,
        &0,
        "unexpected new quarantine candidate count",
    )?;
    ensure_eq(
        &report.retained_quarantine_candidates,
        &0,
        "unexpected retained quarantine candidate count",
    )?;
    ensure_eq(
        &report.released_quarantine_candidates,
        &0,
        "unexpected released quarantine candidate count",
    )?;
    ensure_eq(&report.deleted_chunks, &0, "unexpected deleted chunk count")?;
    ensure_eq(&report.deleted_bytes, &0, "unexpected deleted byte count")?;
    ensure(
        fs::try_exists(orphan_path).await?,
        "orphan chunk should still exist",
    )?;
    ensure(
        !fs::try_exists(quarantine_root(storage.path())).await?,
        "dry run should not create quarantine state",
    )?;

    Ok(())
}

async fn exercise_gc_mark_only_creates_quarantine_candidates() -> Result<(), Box<dyn Error>> {
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

    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;

    ensure_eq(&report.orphan_chunks, &1, "unexpected orphan chunk count")?;
    ensure_eq(
        &report.active_quarantine_candidates,
        &1,
        "unexpected active quarantine candidate count",
    )?;
    ensure_eq(
        &report.new_quarantine_candidates,
        &1,
        "unexpected new quarantine candidate count",
    )?;
    ensure_eq(
        &report.retained_quarantine_candidates,
        &0,
        "unexpected retained quarantine candidate count",
    )?;
    ensure_eq(
        &report.released_quarantine_candidates,
        &0,
        "unexpected released quarantine candidate count",
    )?;
    ensure_eq(&report.deleted_chunks, &0, "unexpected deleted chunk count")?;
    ensure(
        fs::try_exists(orphan_path).await?,
        "orphan chunk should still exist",
    )?;
    ensure(
        IndexStore::quarantine_candidate(
            &LocalIndexStore::open(storage.path().to_path_buf()),
            &chunk_object_key(&orphan_hash)?,
        )?
        .is_some(),
        "mark run should create a quarantine candidate",
    )?;

    Ok(())
}

async fn exercise_gc_mark_only_reaches_a_fixed_point_on_second_run() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;

    let first_report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;
    let second_report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;

    ensure_eq(
        &first_report.new_quarantine_candidates,
        &1,
        "first mark run should create one quarantine candidate",
    )?;
    ensure_eq(
        &second_report.new_quarantine_candidates,
        &0,
        "second mark run should not create an already-tracked candidate",
    )?;
    ensure_eq(
        &second_report.retained_quarantine_candidates,
        &1,
        "second mark run should retain the existing candidate",
    )?;
    ensure_eq(
        &second_report.active_quarantine_candidates,
        &1,
        "second mark run should keep exactly one active candidate",
    )?;
    ensure_eq(
        &second_report.released_quarantine_candidates,
        &0,
        "second mark run should not release a still-orphaned candidate",
    )?;

    Ok(())
}

async fn exercise_gc_diagnostics_include_retention_and_orphan_state() -> Result<(), Box<dyn Error>>
{
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;

    ensure_eq(
        &diagnostics.report.active_quarantine_candidates,
        &1,
        "expected one active quarantine candidate",
    )?;
    ensure_eq(
        &diagnostics.retention_report.len(),
        &1_usize,
        "expected one retention report entry",
    )?;
    ensure_eq(
        &diagnostics.orphan_inventory.len(),
        &1_usize,
        "expected one orphan inventory entry",
    )?;
    let retention_entry = diagnostics
        .retention_report
        .first()
        .ok_or_else(|| ServerTestInvariantError::new("missing retention entry"))?;
    ensure_eq(
        &retention_entry.hash,
        &orphan_hash,
        "unexpected retention hash",
    )?;
    ensure(
        !retention_entry.expired,
        "newly quarantined object should not already be expired",
    )?;
    ensure(
        retention_entry.seconds_until_delete <= 3_600,
        "retention window should not exceed requested duration",
    )?;

    let orphan_entry = diagnostics
        .orphan_inventory
        .first()
        .ok_or_else(|| ServerTestInvariantError::new("missing orphan entry"))?;
    ensure_eq(&orphan_entry.hash, &orphan_hash, "unexpected orphan hash")?;
    ensure_eq(
        &orphan_entry.quarantine_state,
        &GcOrphanQuarantineState::Quarantined,
        "orphan should be reported as quarantined after mark run",
    )?;
    ensure(
        orphan_entry
            .first_seen_unreachable_at_unix_seconds
            .is_some(),
        "quarantined orphan should include first-seen timestamp",
    )?;
    ensure(
        orphan_entry.delete_after_unix_seconds.is_some(),
        "quarantined orphan should include delete-after timestamp",
    )?;

    Ok(())
}

async fn exercise_gc_sweep_only_deletes_expired_quarantine_candidates() -> Result<(), Box<dyn Error>>
{
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let object_key = chunk_object_key(&orphan_hash)?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        object_key.clone(),
        6,
        1,
        1,
    )?)?;

    let report = run_local_gc(storage.path().to_path_buf(), LocalGcOptions::sweep_only()).await?;

    ensure_eq(&report.orphan_chunks, &1, "unexpected orphan chunk count")?;
    ensure_eq(&report.deleted_chunks, &1, "unexpected deleted chunk count")?;
    ensure_eq(&report.deleted_bytes, &6, "unexpected deleted byte count")?;
    ensure_eq(
        &report.active_quarantine_candidates,
        &0,
        "unexpected active quarantine candidate count",
    )?;
    ensure_eq(
        &report.released_quarantine_candidates,
        &1,
        "unexpected released quarantine candidate count",
    )?;
    ensure(
        !fs::try_exists(orphan_path).await?,
        "expired quarantine candidate should be deleted",
    )?;
    ensure(
        IndexStore::quarantine_candidate(&index_store, &object_key)?.is_none(),
        "sweep should remove the quarantine candidate",
    )?;

    Ok(())
}

async fn exercise_gc_releases_quarantine_candidates_when_chunk_becomes_reachable()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_bytes = b"bbbb";
    let orphan_path = write_orphan_chunk(storage.path(), "temporary", orphan_bytes).await?;
    let Some(hash) = orphan_path.file_name().and_then(|name| name.to_str()) else {
        return Err(ServerTestInvariantError::new("orphan hash file name was invalid").into());
    };
    let hash = hash.to_owned();

    let first_report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;
    ensure_eq(
        &first_report.active_quarantine_candidates,
        &1,
        "mark run should create one active quarantine candidate",
    )?;

    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset.bin", Bytes::from_static(orphan_bytes), None)
        .await?;

    let second_report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_only(3_600),
    )
    .await?;
    ensure_eq(
        &second_report.orphan_chunks,
        &0,
        "chunk should become reachable",
    )?;
    ensure_eq(
        &second_report.active_quarantine_candidates,
        &0,
        "reachable chunk should not stay quarantined",
    )?;
    ensure_eq(
        &second_report.released_quarantine_candidates,
        &1,
        "mark run should release the quarantine candidate",
    )?;
    ensure(
        fs::try_exists(storage.path().join("chunks").join(&hash[..2]).join(&hash)).await?,
        "reachable chunk should still exist on disk",
    )?;
    ensure(
        IndexStore::quarantine_candidate(
            &LocalIndexStore::open(storage.path().to_path_buf()),
            &chunk_object_key(&hash)?,
        )?
        .is_none(),
        "reachable chunk should no longer have a quarantine candidate",
    )?;

    Ok(())
}

async fn exercise_gc_mark_and_sweep_with_zero_retention_deletes_new_candidates()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    ensure_eq(
        &report.new_quarantine_candidates,
        &1,
        "mark and sweep should create one new candidate",
    )?;
    ensure_eq(
        &report.deleted_chunks,
        &1,
        "mark and sweep should delete the orphan",
    )?;
    ensure_eq(
        &report.active_quarantine_candidates,
        &0,
        "mark and sweep should leave no active candidates",
    )?;
    ensure_eq(
        &report.released_quarantine_candidates,
        &1,
        "mark and sweep should release the deleted candidate",
    )?;
    ensure(
        !fs::try_exists(orphan_path).await?,
        "zero retention mark and sweep should delete the orphan chunk",
    )?;

    Ok(())
}

async fn exercise_gc_active_retention_holds_exclude_chunks_from_orphans()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let object_key = chunk_object_key(&orphan_hash)?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let hold = RetentionHold::new(
        object_key.clone(),
        "provider deletion grace".to_owned(),
        1,
        None,
    )?;
    index_store.upsert_retention_hold(&hold)?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &0,
        "held chunk should not count as orphaned",
    )?;
    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &0,
        "held chunk should not be deleted",
    )?;
    ensure_eq(
        &diagnostics.report.active_quarantine_candidates,
        &0,
        "held chunk should not be quarantined",
    )?;
    ensure_eq(
        &diagnostics.report.released_quarantine_candidates,
        &0,
        "held chunk should not release quarantine state when none existed",
    )?;
    ensure_eq(
        &diagnostics.retention_report.len(),
        &0_usize,
        "held chunk should not appear in the orphan retention report",
    )?;
    ensure_eq(
        &diagnostics.orphan_inventory.len(),
        &0_usize,
        "held chunk should not appear in the orphan inventory",
    )?;
    ensure(
        fs::try_exists(orphan_path).await?,
        "held chunk should still exist on disk",
    )?;

    Ok(())
}

async fn exercise_gc_dry_run_keeps_expired_retention_holds() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "ef".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let object_key = chunk_object_key(&orphan_hash)?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let expired_hold = RetentionHold::new(
        object_key.clone(),
        "expired provider deletion grace".to_owned(),
        1,
        Some(1),
    )?;
    index_store.upsert_retention_hold(&expired_hold)?;

    let diagnostics =
        run_local_gc_diagnostics(storage.path().to_path_buf(), LocalGcOptions::dry_run()).await?;

    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &1,
        "expired hold should not exclude the orphan from accounting",
    )?;
    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &0,
        "dry run should not delete the orphan",
    )?;
    ensure_eq(
        &diagnostics.retention_report.len(),
        &0_usize,
        "expired hold should not appear in the active retention report",
    )?;
    ensure(
        IndexStore::retention_hold(&index_store, &object_key)?.is_some(),
        "dry run should not prune expired hold metadata",
    )?;

    Ok(())
}

async fn exercise_gc_mutating_run_prunes_expired_retention_holds() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "ef".repeat(32);
    write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let object_key = chunk_object_key(&orphan_hash)?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let expired_hold = RetentionHold::new(
        object_key.clone(),
        "expired provider deletion grace".to_owned(),
        1,
        Some(1),
    )?;
    index_store.upsert_retention_hold(&expired_hold)?;

    let diagnostics =
        run_local_gc_diagnostics(storage.path().to_path_buf(), LocalGcOptions::mark_only(60))
            .await?;

    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &1,
        "expired hold should not exclude the orphan from accounting",
    )?;
    ensure_eq(
        &diagnostics.retention_report.len(),
        &1_usize,
        "mark run should quarantine the orphan after expired hold pruning",
    )?;
    ensure(
        IndexStore::retention_hold(&index_store, &object_key)?.is_none(),
        "mutating gc run should prune expired hold metadata",
    )?;

    Ok(())
}

async fn exercise_gc_mark_and_sweep_deletes_orphan_native_xorb_object() -> Result<(), Box<dyn Error>>
{
    let storage = tempfile::tempdir()?;
    let xorb_key = xorb_object_key(&"ab".repeat(32))?;
    let xorb_path =
        write_orphan_object(storage.path(), &xorb_key, b"serialized native xorb").await?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &1,
        "orphan native xorb object should be deleted",
    )?;
    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &1,
        "orphan native xorb object should count as orphaned",
    )?;
    ensure(
        !fs::try_exists(xorb_path).await?,
        "orphan native xorb object should be removed",
    )?;

    Ok(())
}

async fn exercise_gc_mark_and_sweep_deletes_orphan_retained_shard_object()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let shard_key = shard_object_key(&"cd".repeat(32))?;
    let shard_path =
        write_orphan_object(storage.path(), &shard_key, b"serialized retained shard").await?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &1,
        "orphan retained shard object should be deleted",
    )?;
    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &1,
        "orphan retained shard object should count as orphaned",
    )?;
    ensure(
        !fs::try_exists(shard_path).await?,
        "orphan retained shard object should be removed",
    )?;

    Ok(())
}

async fn exercise_gc_stale_dedupe_mapping_does_not_keep_retained_shard_alive()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let chunk_hash = "de".repeat(32);
    let shard_key = shard_object_key(&"cd".repeat(32))?;
    let shard_path =
        write_orphan_object(storage.path(), &shard_key, b"serialized retained shard").await?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let chunk_hash = ShardlineHash::parse_api_hex(&chunk_hash)?;
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping)?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &1,
        "stale dedupe mapping should not keep retained shard alive",
    )?;
    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &1,
        "retained shard behind stale dedupe mapping should count as orphaned",
    )?;
    ensure(
        !fs::try_exists(shard_path).await?,
        "retained shard behind stale dedupe mapping should be removed",
    )?;

    Ok(())
}

async fn exercise_gc_live_native_xet_record_keeps_retained_shard_reachable()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).ok_or("chunk size")?,
    )
    .await?;
    let (xorb_body, xorb_hash_hex) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash_hex)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash_hex, xorb_body).await?;
    ensure(
        uploaded_xorb.was_inserted,
        "expected xorb upload to insert object",
    )?;
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body.clone()),
            None,
            ShardMetadataLimits::default(),
        )
        .await?;
    ensure_eq(
        &uploaded_shard.result,
        &1,
        "expected shard registration to index one file",
    )?;
    let shard_hash = shard_hash_hex(shard_body.as_ref());
    let shard_key = shard_object_key(&shard_hash)?;
    let shard_path = storage.path().join("chunks").join(shard_key.as_str());

    let diagnostics =
        run_local_gc_diagnostics(storage.path().to_path_buf(), LocalGcOptions::dry_run()).await?;

    ensure_eq(
        &diagnostics.report.orphan_chunks,
        &0,
        "live native xet retained shard should not count as orphaned",
    )?;
    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &0,
        "dry run must not delete live retained shard",
    )?;
    ensure(
        fs::try_exists(shard_path).await?,
        "live native xet retained shard should remain on disk",
    )?;

    Ok(())
}

async fn exercise_gc_fails_closed_on_corrupt_webhook_delivery_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-corrupt".to_owned(),
        100,
    )?;
    index_store.record_webhook_delivery(&delivery)?;
    let connection = Connection::open(storage.path().join("metadata.sqlite3"))?;
    connection.execute(
        "UPDATE shardline_webhook_deliveries
             SET provider = ?1
             WHERE provider = ?2 AND owner = ?3 AND repo = ?4 AND delivery_id = ?5",
        params![
            "invalid-provider",
            "github",
            delivery.owner(),
            delivery.repo(),
            delivery.delivery_id(),
        ],
    )?;

    let result = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await;
    let Some(error) = result.err() else {
        return Err(ServerTestInvariantError::new(
            "gc unexpectedly succeeded on corrupt webhook metadata",
        )
        .into());
    };
    let expected_error = String::from("index adapter operation failed");
    ensure_eq(
        &format!("{error}"),
        &expected_error,
        "gc should fail through index adapter errors",
    )?;
    ensure_eq(
        &connection.query_row(
            "SELECT provider
                 FROM shardline_webhook_deliveries
                 WHERE owner = ?1 AND repo = ?2 AND delivery_id = ?3",
            params![delivery.owner(), delivery.repo(), delivery.delivery_id()],
            |row| row.get::<_, String>(0),
        )?,
        &"invalid-provider".to_owned(),
        "gc should leave corrupt webhook metadata unchanged",
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_fails_closed_on_missing_quarantined_object_metadata() {
    let result = exercise_gc_fails_closed_on_missing_quarantined_object_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc should fail closed on missing quarantined object metadata: {error:?}"
    );
}

async fn exercise_gc_fails_closed_on_missing_quarantined_object_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_key = chunk_object_key(&"ab".repeat(32))?;
    let candidate = QuarantineCandidate::new(object_key.clone(), 5, 10, 20)?;
    index_store.upsert_quarantine_candidate(&candidate)?;

    let result = run_local_gc(storage.path().to_path_buf(), LocalGcOptions::mark_only(60)).await;
    assert!(matches!(
        result,
        Err(ServerError::InvalidLifecycleMetadata(_))
    ));
    assert!(IndexStore::quarantine_candidate(&index_store, &object_key)?.is_some());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_fails_closed_on_active_hold_quarantine_conflict() {
    let result = exercise_gc_fails_closed_on_active_hold_quarantine_conflict().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc should fail closed on active hold/quarantine conflict: {error:?}"
    );
}

async fn exercise_gc_fails_closed_on_active_hold_quarantine_conflict() -> Result<(), Box<dyn Error>>
{
    let storage = tempfile::tempdir()?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_key = chunk_object_key(&"cd".repeat(32))?;
    let object_path = write_orphan_object(storage.path(), &object_key, b"held").await?;
    let held_at_unix_seconds = unix_now_seconds_lossy();
    let release_after_unix_seconds = held_at_unix_seconds.checked_add(60);
    assert!(release_after_unix_seconds.is_some());
    let Some(release_after_unix_seconds) = release_after_unix_seconds else {
        return Err(ServerTestInvariantError::new("hold release-after overflowed").into());
    };
    let hold = RetentionHold::new(
        object_key.clone(),
        "hold".to_owned(),
        held_at_unix_seconds,
        Some(release_after_unix_seconds),
    )?;
    let candidate = QuarantineCandidate::new(
        object_key.clone(),
        4,
        held_at_unix_seconds,
        release_after_unix_seconds,
    )?;
    index_store.upsert_retention_hold(&hold)?;
    index_store.upsert_quarantine_candidate(&candidate)?;

    let result = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await;
    assert!(matches!(
        result,
        Err(ServerError::InvalidLifecycleMetadata(_))
    ));
    assert!(IndexStore::retention_hold(&index_store, &object_key)?.is_some());
    assert!(IndexStore::quarantine_candidate(&index_store, &object_key)?.is_some());
    assert!(fs::try_exists(&object_path).await?);

    Ok(())
}

async fn write_orphan_chunk(
    root: &Path,
    hash_seed: &str,
    bytes: &[u8],
) -> Result<PathBuf, Box<dyn Error>> {
    let hash = if hash_seed.len() == 64 && hash_seed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        hash_seed.to_owned()
    } else {
        chunk_hash(bytes).api_hex_string()
    };
    let path = root.join("chunks").join(&hash[..2]).join(&hash);
    let Some(parent) = path.parent() else {
        return Err(ServerTestInvariantError::new("orphan parent directory missing").into());
    };
    fs::create_dir_all(parent).await?;
    fs::write(&path, bytes).await?;
    Ok(path)
}

async fn write_orphan_object(
    root: &Path,
    object_key: &ObjectKey,
    bytes: &[u8],
) -> Result<PathBuf, Box<dyn Error>> {
    let path = root.join("chunks").join(object_key.as_str());
    let Some(parent) = path.parent() else {
        return Err(ServerTestInvariantError::new("orphan object parent directory missing").into());
    };
    fs::create_dir_all(parent).await?;
    fs::write(&path, bytes).await?;
    Ok(path)
}

async fn write_quarantine_manifest(
    path: &Path,
    record: &QuarantineRecord,
) -> Result<(), Box<dyn Error>> {
    let Some(parent) = path.parent() else {
        return Err(ServerTestInvariantError::new("quarantine parent directory missing").into());
    };
    fs::create_dir_all(parent).await?;
    fs::write(path, to_vec(record)?).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reads_legacy_quarantine_manifest_format() {
    let result = exercise_gc_reads_legacy_quarantine_manifest_format().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc reads legacy quarantine manifest format failed: {error:?}"
    );
}

async fn exercise_gc_reads_legacy_quarantine_manifest_format() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let quarantine_path = quarantine_record_path(&quarantine_root(storage.path()), &orphan_hash);
    write_quarantine_manifest(
        &quarantine_path,
        &QuarantineRecord {
            hash: orphan_hash.clone(),
            bytes: 6,
            first_seen_unreachable_at_unix_seconds: 1,
            delete_after_unix_seconds: 1,
        },
    )
    .await?;

    let report = run_local_gc(storage.path().to_path_buf(), LocalGcOptions::sweep_only()).await?;

    ensure_eq(
        &report.deleted_chunks,
        &1,
        "legacy candidate should still sweep",
    )?;
    ensure(
        !fs::try_exists(orphan_path).await?,
        "legacy quarantine manifest should still delete the orphan chunk",
    )?;

    Ok(())
}

#[test]
fn quarantine_record_path_matches_chunk_object_key_layout() {
    let hash = "de".repeat(32);
    let key = chunk_object_key(&hash);

    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let expected_path = PathBuf::from("gc")
        .join("quarantine")
        .join(key.as_str())
        .with_extension("json");

    assert_eq!(
        quarantine_record_path(Path::new("gc/quarantine"), &hash),
        expected_path
    );
}

fn ensure(condition: bool, message: &str) -> Result<(), Box<dyn Error>> {
    if condition {
        return Ok(());
    }

    Err(ServerTestInvariantError::new(message).into())
}

fn ensure_eq<T>(actual: &T, expected: &T, message: &str) -> Result<(), Box<dyn Error>>
where
    T: PartialEq + Debug,
{
    if actual == expected {
        return Ok(());
    }

    Err(
        ServerTestInvariantError::new(format!("{message}: expected {expected:?}, got {actual:?}"))
            .into(),
    )
}
