//! Integration tests for `shardline-gc`.
//!
//! These tests exercise the public API (`run_local_gc`, `run_gc_with_stores`,
//! `run_local_gc_diagnostics`) against in-memory and filesystem-backed stores
//! in various states:
//!
//! - Empty store → zeroed report
//! - Dry-run mode with orphan chunks → detected but not deleted
//! - Mark-only → quarantine entries created
//! - Mark-and-sweep → orphan chunks deleted
//! - Valid file records referencing chunks → no orphans
//! - GC diagnostics → full diagnostics struct populated
//! - Multiple frontend configurations
//! - Quarantine lifecycle (create → retain → release)

// Test code — allow panicky helpers that are standard in test fixtures.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use std::path::PathBuf;

use shardline_gc::{
    LocalGcOptions, ServerFrontend, quarantine_record_path, quarantine_root, run_gc_with_stores,
    run_local_gc, run_local_gc_diagnostics,
};
use shardline_index::{
    AsyncIndexStore, FileChunkRecord, FileRecord, LifecycleStore, MemoryIndexStore,
    MemoryRecordStore, QuarantineCandidate, RecordMutation,
};
use shardline_server_core::{ServerObjectStore, chunk_hash, chunk_object_key};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

// ============================================================================
// Helpers
// ============================================================================

/// Creates a temporary directory and returns the root path.
fn temp_root() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("chunks")).unwrap();
    (dir, root)
}

/// Puts an object into the object store.
fn put_object(object_store: &ServerObjectStore, key: &ObjectKey, data: &[u8]) {
    let hash = chunk_hash(data);
    let integrity = ObjectIntegrity::new(hash, u64::try_from(data.len()).unwrap_or(0));
    object_store
        .put_if_absent(key, ObjectBody::Borrowed(data), &integrity)
        .unwrap();
}

/// Creates a hex hash string of length 64 by repeating a 2-char prefix 32 times.
fn make_hash(prefix: &str) -> String {
    assert_eq!(prefix.len(), 2);
    prefix.repeat(32)
}

/// Creates a chunk object key from a hex hash.
fn chunk_key(hash: &str) -> ObjectKey {
    chunk_object_key(hash).unwrap()
}

/// Helper: runs `run_gc_with_stores` with in-memory stores and a local object store.
async fn run_gc_helper(
    object_store: &ServerObjectStore,
    index_store: &MemoryIndexStore,
    record_store: &MemoryRecordStore,
    options: LocalGcOptions,
) -> shardline_gc::LocalGcDiagnostics {
    run_gc_with_stores(
        record_store,
        index_store,
        object_store,
        &[ServerFrontend::Xet],
        options,
    )
    .await
    .expect("GC should succeed")
}

// ============================================================================
// run_local_gc — filesystem-backed end-to-end tests
// ============================================================================

#[test]
fn empty_store_dry_run() {
    let (_dir, root) = temp_root();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt.block_on(run_local_gc(root, LocalGcOptions::dry_run()));
    assert!(report.is_ok(), "dry-run on empty store: {:?}", report);
    let report = report.unwrap();
    assert_eq!(report.scanned_records, 0);
    assert_eq!(report.referenced_chunks, 0);
    assert_eq!(report.orphan_chunks, 0);
    assert_eq!(report.orphan_chunk_bytes, 0);
    assert_eq!(report.active_quarantine_candidates, 0);
    assert_eq!(report.new_quarantine_candidates, 0);
    assert_eq!(report.retained_quarantine_candidates, 0);
    assert_eq!(report.released_quarantine_candidates, 0);
    assert_eq!(report.deleted_chunks, 0);
    assert_eq!(report.deleted_bytes, 0);
}

#[test]
fn dry_run_detects_orphans_does_not_delete() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash = make_hash("aa");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"orphan data for dry-run");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(run_local_gc(root.clone(), LocalGcOptions::dry_run()))
        .expect("dry-run should succeed");

    assert_eq!(report.orphan_chunks, 1, "orphan chunk should be detected");
    assert!(report.orphan_chunk_bytes > 0, "orphan bytes should be > 0");
    assert_eq!(report.deleted_chunks, 0, "dry-run should not delete");
    assert_eq!(report.deleted_bytes, 0, "dry-run should not delete bytes");
    assert_eq!(
        report.new_quarantine_candidates, 0,
        "dry-run should not mark"
    );
    assert_eq!(
        report.active_quarantine_candidates, 0,
        "dry-run should not create quarantine"
    );

    // Verify the chunk still exists on disk.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    assert!(
        object_store.contains(&key).unwrap(),
        "orphan should still exist after dry-run"
    );
}

#[test]
fn local_gc_mark_only_creates_quarantine_entries() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash = make_hash("bb");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"quarantine-me");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(run_local_gc(
            root.clone(),
            LocalGcOptions::mark_only(86_400),
        ))
        .expect("mark should succeed");

    assert_eq!(report.orphan_chunks, 1, "orphan should be detected in mark");
    assert_eq!(
        report.new_quarantine_candidates, 1,
        "new quarantine entry should be created"
    );
    assert_eq!(
        report.active_quarantine_candidates, 1,
        "active candidates should be 1"
    );
    assert_eq!(report.deleted_chunks, 0, "mark should not delete");

    // Verify the chunk still exists.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    assert!(object_store.contains(&key).unwrap());
}

#[test]
fn local_gc_mark_and_sweep_deletes_orphans() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash = make_hash("cc");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"delete-me");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(run_local_gc(
            root.clone(),
            LocalGcOptions::mark_and_sweep(0),
        ))
        .expect("mark-and-sweep should succeed");

    assert_eq!(
        report.new_quarantine_candidates, 1,
        "new quarantine entry created"
    );
    assert_eq!(
        report.deleted_chunks, 1,
        "orphan should be deleted by sweep"
    );
    assert!(report.deleted_bytes > 0, "deleted_bytes should be > 0");
    assert_eq!(
        report.active_quarantine_candidates, 0,
        "quarantine should be empty after sweep"
    );

    // Verify the chunk is gone.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    assert!(
        !object_store.contains(&key).unwrap(),
        "orphan should be deleted"
    );
}

#[test]
fn local_gc_with_valid_record_no_orphans() {
    let (_dir, root) = temp_root();
    let record_store =
        shardline_index::LocalRecordStore::new(root.clone()).expect("create local record store");
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash = make_hash("dd");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"referenced chunk");

    let record = FileRecord {
        file_id: "test-file".to_owned(),
        content_hash: hash.clone(),
        total_bytes: 17,
        chunk_size: 100,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash,
            offset: 0,
            length: 17,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(record_store.commit_file_version_metadata(&record))
        .expect("commit record");

    drop(record_store);
    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(run_local_gc(root, LocalGcOptions::dry_run()))
        .expect("GC should succeed");

    assert_eq!(report.scanned_records, 2, "version + latest = 2 records");
    assert_eq!(report.referenced_chunks, 1, "one chunk referenced");
    assert_eq!(
        report.orphan_chunks, 0,
        "no orphans when record references chunk"
    );
    assert_eq!(report.deleted_chunks, 0);
}

// ============================================================================
// run_gc_with_stores — in-memory store tests
// ============================================================================

#[test]
fn gc_with_stores_empty_store() {
    let object_store = ServerObjectStore::blackhole();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::dry_run(),
    ));

    assert_eq!(diag.report.scanned_records, 0);
    assert_eq!(diag.report.orphan_chunks, 0);
    assert_eq!(diag.report.deleted_chunks, 0);
    assert!(diag.retention_report.is_empty());
    assert!(diag.orphan_inventory.is_empty());
}

#[test]
fn gc_with_stores_dry_run_detects_orphans() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("ee");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"orphan detected");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::dry_run(),
    ));

    assert_eq!(diag.report.orphan_chunks, 1);
    assert_eq!(diag.report.deleted_chunks, 0);
    assert_eq!(diag.report.new_quarantine_candidates, 0);
    assert_eq!(diag.orphan_inventory.len(), 1);
    assert_eq!(diag.orphan_inventory[0].object_key, key.as_str());
    assert_eq!(diag.orphan_inventory[0].bytes, 15); // "orphan detected"
    assert_eq!(
        diag.orphan_inventory[0].quarantine_state,
        shardline_gc::GcOrphanQuarantineState::Untracked
    );
}

#[test]
fn gc_with_stores_mark_only_creates_quarantine() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("ff");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"mark as quarantine");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::mark_only(3600),
    ));

    assert_eq!(diag.report.new_quarantine_candidates, 1);
    assert_eq!(diag.report.active_quarantine_candidates, 1);
    assert_eq!(diag.report.deleted_chunks, 0);
    assert_eq!(diag.retention_report.len(), 1);

    // Verify the candidate was persisted in the index store.
    let candidates = LifecycleStore::list_quarantine_candidates(&index_store).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].object_key().as_str(), key.as_str());
    assert_eq!(candidates[0].observed_length(), 18); // "mark as quarantine"

    // Verify the chunk still exists.
    assert!(object_store.contains(&key).unwrap());
}

#[test]
fn gc_with_stores_mark_and_sweep_deletes_orphans() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("11");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"sweep away");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::mark_and_sweep(0),
    ));

    assert_eq!(diag.report.new_quarantine_candidates, 1);
    assert_eq!(diag.report.deleted_chunks, 1);
    assert!(diag.report.deleted_bytes > 0);
    assert_eq!(diag.report.active_quarantine_candidates, 0);

    // Chunk should be deleted from object store.
    assert!(!object_store.contains(&key).unwrap());

    // Quarantine entry should be cleaned up.
    let candidates = LifecycleStore::list_quarantine_candidates(&index_store).unwrap();
    assert!(candidates.is_empty());
}

#[test]
fn gc_with_stores_referenced_chunks_not_orphaned() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("22");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"referenced");

    // Create a record that references this chunk.
    let record = FileRecord {
        file_id: "file-1".to_owned(),
        content_hash: hash.clone(),
        total_bytes: 10,
        chunk_size: 100,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash,
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(record_store.write_latest_record(&record))
        .unwrap();

    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::dry_run(),
    ));

    assert_eq!(diag.report.scanned_records, 1);
    assert_eq!(diag.report.referenced_chunks, 1);
    assert_eq!(diag.report.orphan_chunks, 0);
}

#[test]
fn gc_with_stores_multiple_chunks_some_orphaned() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    // Referenced chunk.
    let ref_hash = make_hash("33");
    let ref_key = chunk_key(&ref_hash);
    put_object(&object_store, &ref_key, b"referenced chunk data");

    // Orphan chunk (not referenced by any record).
    let orphan_hash = make_hash("44");
    let orphan_key = chunk_key(&orphan_hash);
    put_object(&object_store, &orphan_key, b"orphan chunk data");

    // Create a record referencing only the first chunk.
    let record = FileRecord {
        file_id: "file-with-chunks".to_owned(),
        content_hash: ref_hash.clone(),
        total_bytes: 21,
        chunk_size: 100,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: ref_hash,
            offset: 0,
            length: 21,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(record_store.write_latest_record(&record))
        .unwrap();

    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::dry_run(),
    ));

    assert_eq!(diag.report.scanned_records, 1);
    assert_eq!(diag.report.referenced_chunks, 1);
    assert_eq!(diag.report.orphan_chunks, 1, "orphan chunk detected");
    assert_eq!(diag.orphan_inventory.len(), 1);
    assert_eq!(diag.orphan_inventory[0].object_key, orphan_key.as_str());
}

// ============================================================================
// run_local_gc_diagnostics tests
// ============================================================================

#[test]
fn diagnostics_empty_store() {
    let (_dir, root) = temp_root();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let diagnostics = rt
        .block_on(run_local_gc_diagnostics(root, LocalGcOptions::dry_run()))
        .expect("diagnostics should succeed");

    assert_eq!(diagnostics.report.scanned_records, 0);
    assert!(diagnostics.retention_report.is_empty());
    assert!(diagnostics.orphan_inventory.is_empty());
}

#[test]
fn diagnostics_with_multiple_orphans() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash_a = make_hash("55");
    let hash_b = make_hash("66");
    let key_a = chunk_key(&hash_a);
    let key_b = chunk_key(&hash_b);
    put_object(&object_store, &key_a, b"first orphan");
    put_object(&object_store, &key_b, b"second orphan data");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diagnostics = rt
        .block_on(run_local_gc_diagnostics(root, LocalGcOptions::dry_run()))
        .expect("diagnostics with orphans should succeed");

    assert_eq!(diagnostics.report.orphan_chunks, 2);
    assert!(diagnostics.report.orphan_chunk_bytes > 0);
    assert_eq!(diagnostics.orphan_inventory.len(), 2);

    // Inventory should be sorted by object_key.
    assert_eq!(diagnostics.orphan_inventory[0].object_key, key_a.as_str());
    assert_eq!(diagnostics.orphan_inventory[1].object_key, key_b.as_str());

    // Both should be Untracked (not quarantined).
    for entry in &diagnostics.orphan_inventory {
        assert_eq!(
            entry.quarantine_state,
            shardline_gc::GcOrphanQuarantineState::Untracked
        );
        assert!(entry.first_seen_unreachable_at_unix_seconds.is_none());
        assert!(entry.delete_after_unix_seconds.is_none());
    }
}

#[test]
fn diagnostics_mark_only_retention_report() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash = make_hash("77");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"retention report test");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let diagnostics = rt
        .block_on(run_local_gc_diagnostics(
            root,
            LocalGcOptions::mark_only(86_400),
        ))
        .expect("diagnostics with mark should succeed");

    assert_eq!(diagnostics.report.new_quarantine_candidates, 1);
    assert_eq!(diagnostics.report.active_quarantine_candidates, 1);
    assert_eq!(diagnostics.retention_report.len(), 1);

    let entry = &diagnostics.retention_report[0];
    assert_eq!(entry.object_key, key.as_str());
    assert!(!entry.expired, "fresh quarantine should not be expired");
    assert_eq!(entry.observed_length, 21);
    assert_eq!(
        entry.first_seen_unreachable_at_unix_seconds,
        entry.delete_after_unix_seconds - 86_400
    );

    // Orphan inventory should show Quarantined state.
    assert_eq!(diagnostics.orphan_inventory.len(), 1);
    assert_eq!(
        diagnostics.orphan_inventory[0].quarantine_state,
        shardline_gc::GcOrphanQuarantineState::Quarantined
    );
    assert!(
        diagnostics.orphan_inventory[0]
            .first_seen_unreachable_at_unix_seconds
            .is_some()
    );
    assert!(
        diagnostics.orphan_inventory[0]
            .delete_after_unix_seconds
            .is_some()
    );
}

// ============================================================================
// Quarantine lifecycle tests
// ============================================================================

#[test]
fn quarantine_mark_then_sweep_retains_unexpired_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("88");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"quarantine lifecycle");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Mark — create quarantine entry with long retention.
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::mark_only(86_400),
    ));
    assert_eq!(diag.report.new_quarantine_candidates, 1);
    assert_eq!(diag.report.active_quarantine_candidates, 1);

    // Sweep (retention not expired) — should retain.
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::sweep_only(),
    ));
    assert_eq!(diag.report.deleted_chunks, 0);
    assert_eq!(
        diag.report.active_quarantine_candidates, 1,
        "unexpired quarantine should be retained"
    );
    assert!(
        object_store.contains(&key).unwrap(),
        "chunk should still exist"
    );
}

#[test]
fn quarantine_mark_and_sweep_with_zero_retention_deletes_immediately() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let hash = make_hash("99");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"immediate delete");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Mark-and-sweep with 0 retention: create + delete in one pass.
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::mark_and_sweep(0),
    ));
    assert_eq!(diag.report.new_quarantine_candidates, 1);
    assert_eq!(diag.report.deleted_chunks, 1);
    assert!(diag.report.deleted_bytes > 0);
    assert_eq!(diag.report.active_quarantine_candidates, 0);

    // Chunk should be deleted.
    assert!(
        !object_store.contains(&key).unwrap(),
        "chunk should be deleted by sweep"
    );
}

#[test]
fn pre_existing_quarantine_candidate_released_when_missing_from_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    // Create a quarantine candidate for an object that doesn't exist.
    let hash = make_hash("aa");
    let key = chunk_key(&hash);
    let candidate = QuarantineCandidate::new(key, 100, 1_000_000, 2_000_000).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(AsyncIndexStore::upsert_quarantine_candidate(
        &index_store,
        &candidate,
    ))
    .unwrap();

    // Run GC — auto-release happens during validate_gc_index_integrity.
    let result = rt.block_on(run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        LocalGcOptions::dry_run(),
    ));
    assert!(
        result.is_ok(),
        "auto-release of missing candidate should succeed: {:?}",
        result
    );

    // Verify the candidate was removed from the index store.
    let candidates = rt
        .block_on(AsyncIndexStore::list_quarantine_candidates(&index_store))
        .unwrap();
    assert!(
        candidates.is_empty(),
        "quarantine candidate should have been auto-released"
    );
}

// ============================================================================
// Frontend configuration tests
// ============================================================================

#[test]
fn gc_with_no_frontends_succeeds() {
    let object_store = ServerObjectStore::blackhole();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[],
        LocalGcOptions::dry_run(),
    ));
    assert!(
        result.is_ok(),
        "empty frontends should not error: {:?}",
        result
    );
}

#[test]
fn gc_with_multiple_frontends_succeeds() {
    let object_store = ServerObjectStore::blackhole();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet, ServerFrontend::Lfs],
        LocalGcOptions::dry_run(),
    ));
    assert!(result.is_ok(), "multiple frontends: {:?}", result);
}

// ============================================================================
// Public utility function tests
// ============================================================================

#[test]
fn quarantine_root_returns_correct_path() {
    let result = quarantine_root(std::path::Path::new("/data"));
    assert_eq!(result, PathBuf::from("/data/gc/quarantine"));
}

#[test]
fn quarantine_record_path_returns_correct_path() {
    let hash = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
    let result = quarantine_record_path(std::path::Path::new("/root"), hash);
    assert_eq!(
        result,
        PathBuf::from(
            "/root/aa/aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899.json"
        )
    );
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn gc_mark_and_sweep_with_multiple_orphans() {
    let (_dir, root) = temp_root();
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();

    let hash_a = make_hash("ab");
    let hash_b = make_hash("cd");
    let key_a = chunk_key(&hash_a);
    let key_b = chunk_key(&hash_b);
    put_object(&object_store, &key_a, b"orphan-a");
    put_object(&object_store, &key_b, b"orphan-b-data");

    drop(object_store);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let report = rt
        .block_on(run_local_gc(
            root.clone(),
            LocalGcOptions::mark_and_sweep(0),
        ))
        .expect("mark-and-sweep with multiple orphans");

    assert_eq!(report.orphan_chunks, 2);
    assert_eq!(report.new_quarantine_candidates, 2);
    assert_eq!(report.deleted_chunks, 2);
    assert!(report.deleted_bytes > 0);
    assert_eq!(report.active_quarantine_candidates, 0);

    // Both chunks should be gone.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    assert!(!object_store.contains(&key_a).unwrap());
    assert!(!object_store.contains(&key_b).unwrap());
}

#[test]
fn gc_very_small_retention_still_respects_minimum_for_existing_quarantine() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
    let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
    let index_store = MemoryIndexStore::new();
    let record_store = MemoryRecordStore::new();

    let now = shardline_protocol::unix_now_seconds_lossy();
    let hash = make_hash("ef");
    let key = chunk_key(&hash);
    put_object(&object_store, &key, b"small retention test");

    // Pre-create a quarantine candidate that was just created (not expired).
    let candidate = QuarantineCandidate::new(
        key,
        20,
        now,
        now + 3600, // 1 hour from now
    )
    .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(AsyncIndexStore::upsert_quarantine_candidate(
        &index_store,
        &candidate,
    ))
    .unwrap();

    // Sweep should not delete the unexpired candidate.
    let diag = rt.block_on(run_gc_helper(
        &object_store,
        &index_store,
        &record_store,
        LocalGcOptions::sweep_only(),
    ));
    assert_eq!(diag.report.deleted_chunks, 0);
    assert_eq!(diag.report.active_quarantine_candidates, 1);
}
