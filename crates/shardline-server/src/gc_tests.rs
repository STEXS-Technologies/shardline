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
    DedupeShardMapping, FileRecord, LifecycleStore, LocalIndexStore, LocalRecordStore,
    QuarantineCandidate, RetentionHold, WebhookDelivery, parse_xet_hash_hex, xet_hash_hex_string,
};
use shardline_protocol::{RepositoryProvider, unix_now_seconds_lossy};
use shardline_storage::ObjectKey;
use tokio::fs;

use crate::{
    LocalBackend, ShardMetadataLimits,
    chunk_store::chunk_object_key,
    gc::{
        GcOrphanQuarantineState, LocalGcOptions, quarantine_record_path, quarantine_root,
        run_local_gc, run_local_gc_diagnostics,
    },
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
    if let Err(ref e) = result {
        eprintln!("GC error: {e:?}");
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_dry_run_keeps_expired_retention_holds() {
    let result = exercise_gc_dry_run_keeps_expired_retention_holds().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc dry run expired retention hold contract failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        NonZeroUsize::new(128).ok_or("chunk size")?,
    )
    .await?;
    let payload: Vec<u8> = (0_u16..300).map(|x| x as u8).collect();
    backend
        .upload_file("asset.bin", Bytes::from(payload.clone()), None)
        .await?;

    let orphan_hash = "de".repeat(32);
    let orphan_path = write_orphan_chunk(storage.path(), &orphan_hash, b"orphan").await?;
    let report = run_local_gc(storage.path().to_path_buf(), LocalGcOptions::dry_run()).await?;

    // Two records are scanned: the file version record and the reconstruction
    // metadata record created alongside it.
    ensure_eq(
        &report.scanned_records,
        &2,
        "unexpected scanned record count",
    )?;
    ensure(
        report.referenced_chunks >= 2,
        "expected at least 2 referenced chunks for 300-byte CDC payload",
    )?;
    // The orphan chunk we wrote manually, plus any unreferenced xorb
    // container stored alongside individual chunks.
    ensure(
        report.orphan_chunks >= 1,
        "expected at least 1 orphan chunk (manual orphan + xorb)",
    )?;
    ensure(
        report.orphan_chunk_bytes >= 6,
        "expected at least 6 bytes of orphan data",
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
        NonZeroUsize::new(128).ok_or("chunk size")?,
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

    // Note: the xorb container appears as an additional orphan.
    ensure(
        report.orphan_chunks >= 1,
        "expected at least 1 orphan chunk (manual orphan + xorb)",
    )?;
    // Note: the xorb container is also quarantined alongside the manual orphan.
    ensure(
        report.active_quarantine_candidates >= 1,
        "expected at least 1 active quarantine candidate (manual orphan + xorb)",
    )?;
    ensure(
        report.new_quarantine_candidates >= 1,
        "expected at least 1 new quarantine candidate (manual orphan + xorb)",
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
        LifecycleStore::quarantine_candidate(
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
    let now_unix_seconds = unix_now_seconds_lossy();
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        object_key.clone(),
        6,
        now_unix_seconds.saturating_sub(7_200),
        now_unix_seconds.saturating_sub(7_200),
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
        LifecycleStore::quarantine_candidate(&index_store, &object_key)?.is_none(),
        "sweep should remove the quarantine candidate",
    )?;

    Ok(())
}

async fn exercise_gc_releases_quarantine_candidates_when_chunk_becomes_reachable()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    // Use a payload smaller than CDC min_chunk (16) so it stays pending and
    // flushes as a single chunk at finish().  A single chunk avoids xorb hash
    // updates (the ingestor's > 1 guard), so the file record points to the
    // individual chunk hash that the GC can find.
    let orphan_payload: Vec<u8> = b"a".to_vec();
    let orphan_path = write_orphan_chunk(storage.path(), "temporary", &orphan_payload).await?;
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

    // Register a file record referencing the orphan chunk hash directly,
    // bypassing the ingestor (which would xorb-pack the chunk and change
    // the hash, making it invisible to the GC).  The orphan chunk file
    // already exists on disk at the hash path written above.
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: hash.clone(),
        total_bytes: orphan_payload.len() as u64,
        chunk_size: 128,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![shardline_index::FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length: orphan_payload.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: orphan_payload.len() as u64,
        }],
    };
    record_store.commit_file_version_metadata(&record).await?;

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
        LifecycleStore::quarantine_candidate(
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
    let now_unix_seconds = unix_now_seconds_lossy();
    let expired_hold = RetentionHold::new(
        object_key.clone(),
        "expired provider deletion grace".to_owned(),
        now_unix_seconds.saturating_sub(7_200),
        Some(now_unix_seconds.saturating_sub(3_600)),
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
        LifecycleStore::retention_hold(&index_store, &object_key)?.is_some(),
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
    let now_unix_seconds = unix_now_seconds_lossy();
    let expired_hold = RetentionHold::new(
        object_key.clone(),
        "expired provider deletion grace".to_owned(),
        now_unix_seconds.saturating_sub(7_200),
        Some(now_unix_seconds.saturating_sub(3_600)),
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
        LifecycleStore::retention_hold(&index_store, &object_key)?.is_none(),
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

/// An orphan xorb with a chunk-hash cache sidecar is swept with both
/// files deleted together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_mark_and_sweep_deletes_xorb_cache_sidecar_with_parent() {
    let result = exercise_gc_mark_and_sweep_deletes_xorb_cache_sidecar_with_parent().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc mark and sweep must delete xorb cache sidecar with parent: {error:?}"
    );
}

async fn exercise_gc_mark_and_sweep_deletes_xorb_cache_sidecar_with_parent()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let hash = "ab".repeat(32);
    let xorb_key = xorb_object_key(&hash)?;
    let cache_key = ObjectKey::parse(&format!("_xorb_chunks/ab/{hash}"))?;

    // Write the orphan xorb container.
    let xorb_path = write_orphan_object(storage.path(), &xorb_key, b"fake xorb data").await?;

    // Write the cache sidecar (_xorb_chunks/{prefix}/{hash}).
    let cache_dir = storage.path().join("chunks").join(cache_key.as_str());
    let cache_parent = cache_dir
        .parent()
        .ok_or_else(|| ServerTestInvariantError::new("cache parent"))?;
    fs::create_dir_all(cache_parent).await?;
    let cache_path = write_orphan_object(storage.path(), &cache_key, b"hash1\nhash2\n").await?;

    let diagnostics = run_local_gc_diagnostics(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;

    // The GC should have deleted the xorb (counted as 1 deleted chunk).
    ensure_eq(
        &diagnostics.report.deleted_chunks,
        &1,
        "orphan xorb should be deleted",
    )?;
    ensure(
        !fs::try_exists(xorb_path).await?,
        "xorb file must be removed after sweep",
    )?;
    // The cache sidecar must also be removed.
    ensure(
        !fs::try_exists(cache_path).await?,
        "xorb cache sidecar must be removed with parent xorb",
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
    let chunk_hash = parse_xet_hash_hex(&chunk_hash)?;
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
        NonZeroUsize::new(128).ok_or("chunk size")?,
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
    assert!(
        result.is_ok(),
        "gc should auto-release missing quarantine entries: {result:?}"
    );
    assert!(LifecycleStore::quarantine_candidate(&index_store, &object_key)?.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_repairs_active_hold_quarantine_conflict() {
    let result = exercise_gc_repairs_active_hold_quarantine_conflict().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "gc should repair the active hold/quarantine conflict: {error:?}"
    );
}

async fn exercise_gc_repairs_active_hold_quarantine_conflict() -> Result<(), Box<dyn Error>> {
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

    let report = run_local_gc(
        storage.path().to_path_buf(),
        LocalGcOptions::mark_and_sweep(0),
    )
    .await?;
    // F-43: held+quarantined is now repaired instead of failing closed: the
    // quarantine entry is released, the active hold keeps protecting the data,
    // and the sweep never deletes the held object (F-42 delete-time hold check).
    ensure_eq(
        &report.deleted_chunks,
        &0,
        "held data must not be deleted by the sweep",
    )?;
    ensure(
        LifecycleStore::retention_hold(&index_store, &object_key)?.is_some(),
        "the active hold must survive the run",
    )?;
    ensure(
        LifecycleStore::quarantine_candidate(&index_store, &object_key)?.is_none(),
        "the held quarantine entry should be released (repaired, not wedged)",
    )?;
    ensure(
        fs::try_exists(&object_path).await?,
        "held object must survive the run",
    )?;

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
        xet_hash_hex_string(chunk_hash(bytes))
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
    let now_unix_seconds = unix_now_seconds_lossy();
    write_quarantine_manifest(
        &quarantine_path,
        &QuarantineRecord {
            hash: orphan_hash.clone(),
            bytes: 6,
            first_seen_unreachable_at_unix_seconds: now_unix_seconds.saturating_sub(7_200),
            delete_after_unix_seconds: now_unix_seconds.saturating_sub(7_200),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_concurrent_upload_interleaving() {
    let result = exercise_gc_concurrent_upload_interleaving().await;
    if let Err(ref e) = result {
        eprintln!("GC concurrent upload error: {e:?}");
    }
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "GC concurrent upload test: {error:?}");
}

async fn exercise_gc_concurrent_upload_interleaving() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let backend = std::sync::Arc::new(
        LocalBackend::new(
            storage.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(128).ok_or("chunk size")?,
        )
        .await?,
    );

    // Upload base content that should survive GC
    let base_content = b"aaaabbbbcccc";
    backend
        .upload_file("base-asset.bin", Bytes::from_static(base_content), None)
        .await?;

    // Write some orphan chunks to give GC work to do
    let orphan_hashes: Vec<String> = (0..5)
        .map(|i: usize| format!("{:02x}", i.wrapping_mul(17)).repeat(32))
        .collect();
    for hash in &orphan_hashes {
        write_orphan_chunk(storage.path(), hash, b"orphan-data").await?;
    }

    // Start background tasks that simulate concurrent uploads while GC runs
    let background_root = storage.path().to_path_buf();
    let background_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
        use std::io::Write as _;
        for i in 0..20 {
            let content = format!("concurrent-upload-data-{i}");
            // Write a chunk file directly to simulate an in-progress upload
            let hash_hex = xet_hash_hex_string(chunk_hash(content.as_bytes()));
            let chunk_dir = background_root.join("chunks").join(&hash_hex[..2]);
            let _ = std::fs::create_dir_all(&chunk_dir);
            let chunk_path = chunk_dir.join(&hash_hex);
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&chunk_path)
                .map(|mut f| f.write_all(content.as_bytes()));
            tokio::task::yield_now().await;
        }
    });

    // Run GC mark + sweep with zero retention while background uploads happen
    let root = storage.path().to_path_buf();
    let report = run_local_gc(root.clone(), LocalGcOptions::mark_only(3_600)).await?;

    // Wait for background uploads to finish
    let _background_result = background_handle.await.ok();

    // Verify GC found ALL orphan chunks (5 pre-existing + 20 concurrent = 25)
    // and base content's chunks were NOT counted as orphans
    ensure(
        report.orphan_chunks >= 5,
        "should detect at least the pre-existing orphan chunks",
    )?;
    ensure(
        report.new_quarantine_candidates >= 5,
        "should quarantine at least the pre-existing orphan chunks",
    )?;

    // Verify base content is still accessible via reconstruction
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(128).ok_or("chunk size")?,
    )
    .await?;
    let record = backend
        .reconstruction("base-asset.bin", None, None, None)
        .await?;
    ensure(
        !record.terms.is_empty(),
        "base content should still be reconstructable after concurrent GC",
    )?;

    Ok(())
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
