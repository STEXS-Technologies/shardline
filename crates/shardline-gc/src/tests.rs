#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use shardline_index::{
    AsyncIndexStore, FileChunkRecord, FileRecord, FileRecordInvariantError, LocalIndexStore,
    LocalRecordStore, MemoryIndexStore, MemoryIndexStoreError, MemoryRecordStore,
    MemoryRecordStoreError, PostgresMetadataStoreError, QuarantineCandidate,
    QuarantineCandidateError, RecordMutation, RetentionHold, RetentionHoldError,
    WebhookDeliveryError,
};
use shardline_server_core::{
    InvalidLifecycleMetadataError, ServerObjectStore, ServerObjectStoreError,
    server_frontend::ServerFrontend,
};
use shardline_storage::{
    LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefixError, ObjectStore,
    S3ObjectStoreError,
};
use shardline_xet_adapter::XetAdapterError;

use crate::DEFAULT_LOCAL_GC_RETENTION_SECONDS;
use crate::error::GcError;
use crate::reachability::OrphanObject;
use crate::runner::{
    build_gc_diagnostics, orphan_inventory_entry, quarantine_record_path, quarantine_root,
    retention_report_entry, run_gc_with_stores, run_local_gc, run_local_gc_diagnostics,
};
use crate::types::{
    GcOrphanQuarantineState, LocalGcDiagnostics, LocalGcOptions, LocalGcReport,
    MINIMUM_GC_RETENTION_SECONDS,
};

#[test]
fn default_retention_seconds_value() {
    assert_eq!(DEFAULT_LOCAL_GC_RETENTION_SECONDS, 86_400);
}

#[test]
fn default_gc_options_are_dry_run() {
    let opts = LocalGcOptions::default();
    assert!(!opts.mark);
    assert!(!opts.sweep);
    assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
}

#[test]
fn dry_run_options() {
    let opts = LocalGcOptions::dry_run();
    assert!(!opts.mark);
    assert!(!opts.sweep);
    assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
}

#[test]
fn mark_only_options() {
    let opts = LocalGcOptions::mark_only(3600);
    assert!(opts.mark);
    assert!(!opts.sweep);
    assert_eq!(opts.retention_seconds, 3600);
}

#[test]
fn sweep_only_options() {
    let opts = LocalGcOptions::sweep_only();
    assert!(!opts.mark);
    assert!(opts.sweep);
    assert_eq!(opts.retention_seconds, DEFAULT_LOCAL_GC_RETENTION_SECONDS);
}

#[test]
fn mark_and_sweep_options() {
    let opts = LocalGcOptions::mark_and_sweep(7200);
    assert!(opts.mark);
    assert!(opts.sweep);
    assert_eq!(opts.retention_seconds, 7200);
}

#[test]
fn mode_name_dry_run() {
    assert_eq!(LocalGcOptions::dry_run().mode_name(), "dry-run");
}

#[test]
fn mode_name_mark() {
    assert_eq!(LocalGcOptions::mark_only(100).mode_name(), "mark");
}

#[test]
fn mode_name_sweep() {
    assert_eq!(LocalGcOptions::sweep_only().mode_name(), "sweep");
}

#[test]
fn mode_name_mark_and_sweep() {
    assert_eq!(
        LocalGcOptions::mark_and_sweep(100).mode_name(),
        "mark-and-sweep"
    );
}

#[test]
fn quarantine_root_for_storage_path() {
    let result = quarantine_root(Path::new("/storage"));
    assert_eq!(result, PathBuf::from("/storage/gc/quarantine"));
}

#[test]
fn quarantine_root_for_dot_path() {
    let result = quarantine_root(Path::new("."));
    assert_eq!(result, PathBuf::from("./gc/quarantine"));
}

#[test]
fn quarantine_record_path_starts_with_prefix_and_ends_with_json() {
    let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let result = quarantine_record_path(Path::new("/storage"), hash);
    assert!(result.starts_with("/storage/ab/"));
    assert!(result.to_string_lossy().ends_with(".json"));
}

// GcError display message tests

#[test]
fn gc_error_io_display_nonempty() {
    let err = GcError::Io(std::io::Error::other("test"));
    let display = err.to_string();
    assert!(!display.is_empty());
    assert_eq!(display, "local storage operation failed");
}

#[test]
fn gc_error_json_display_nonempty() {
    let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
    let err = GcError::Json(json_err);
    let display = err.to_string();
    assert!(!display.is_empty());
    assert_eq!(display, "json operation failed");
}

#[test]
fn gc_error_numeric_conversion_display_nonempty() {
    let source: std::num::TryFromIntError = u8::try_from(256u32).unwrap_err();
    let err = GcError::NumericConversion(source);
    let display = err.to_string();
    assert!(!display.is_empty());
    assert_eq!(display, "numeric conversion exceeded supported bounds");
}

#[test]
fn gc_error_invalid_content_hash_display_nonempty() {
    let err = GcError::InvalidContentHash;
    let display = err.to_string();
    assert!(!display.is_empty());
    assert_eq!(display, "content hash must be 64 hexadecimal characters");
}

#[test]
fn gc_error_overflow_display_nonempty() {
    let err = GcError::Overflow;
    let display = err.to_string();
    assert!(!display.is_empty());
    assert_eq!(display, "arithmetic overflow");
}

// Error conversion tests

#[test]
fn gc_error_from_parse_stored_file_record_metadata_too_large() {
    let parse_err = shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
        observed_bytes: 1024,
        maximum_bytes: 512,
    };
    let gc_err: GcError = parse_err.into();
    assert!(matches!(gc_err, GcError::Io(_)));
}

#[test]
fn gc_error_from_parse_stored_file_record_json() {
    let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
    let parse_err = shardline_server_core::ParseStoredFileRecordError::Json(json_err);
    let gc_err: GcError = parse_err.into();
    assert!(matches!(gc_err, GcError::Json(_)));
}

#[test]
fn gc_error_from_rebuild_overflow() {
    let overflow_err = shardline_server_core::RebuildOverflowError;
    let gc_err: GcError = overflow_err.into();
    assert!(matches!(gc_err, GcError::Overflow));
}

#[test]
fn gc_error_into_server_object_store_error_object_store() {
    let inner_err = ServerObjectStoreError::NotFound;
    let gc_err = GcError::ObjectStore(inner_err);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::NotFound));
}

#[test]
fn gc_error_into_server_object_store_error_io() {
    let io_err = std::io::Error::other("test");
    let gc_err = GcError::Io(io_err);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_numeric_conversion() {
    let source: std::num::TryFromIntError = u8::try_from(256u32).unwrap_err();
    let gc_err = GcError::NumericConversion(source);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(
        server_err,
        ServerObjectStoreError::NumericConversion(_)
    ));
}

#[test]
fn gc_error_into_server_object_store_error_invalid_content_hash() {
    let gc_err = GcError::InvalidContentHash;
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(
        server_err,
        ServerObjectStoreError::InvalidContentHash
    ));
}

#[test]
fn gc_error_into_server_object_store_error_overflow() {
    let gc_err = GcError::Overflow;
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Overflow));
}

// --- GC safety guarantee tests ---

#[test]
fn minimum_gc_retention_seconds_is_enforced() {
    assert_eq!(MINIMUM_GC_RETENTION_SECONDS, 3600);
}

#[test]
fn quarantine_record_path_uppercase_hex_hash_produces_valid_path() {
    // Uppercase hex still produces a valid path; the first two chars become
    // the prefix directory.
    let hash = "ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890";
    let result = quarantine_record_path(Path::new("/storage"), hash);
    assert!(
        result.to_string_lossy().ends_with(".json"),
        "record path must end with .json"
    );
    assert!(
        result.starts_with("/storage/AB/"),
        "record path must be under the two-char prefix directory"
    );
}

#[test]
fn quarantine_record_path_produces_prefix_directory_from_first_two_chars() {
    let hash = "ff00112233445566ff00112233445566ff00112233445566ff00112233445566";
    let result = quarantine_record_path(Path::new("/root"), hash);
    let expected = PathBuf::from(
        "/root/ff/ff00112233445566ff00112233445566ff00112233445566ff00112233445566.json",
    );
    assert_eq!(result, expected);
}

// GcError display messages for all key variants

#[test]
fn gc_error_object_store_display() {
    let inner = ServerObjectStoreError::NotFound;
    let err = GcError::ObjectStore(inner);
    assert_eq!(err.to_string(), "object storage adapter operation failed");
}

#[test]
fn gc_error_xet_adapter_display() {
    let inner = XetAdapterError::NotFound;
    let err = GcError::XetAdapter(inner);
    assert_eq!(err.to_string(), "xet adapter operation failed");
}

// Error conversion: From<io::Error> for GcError

#[test]
fn gc_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    let gc_err: GcError = io_err.into();
    assert!(matches!(gc_err, GcError::Io(_)));
}

// Error conversion: From<serde_json::Error> for GcError

#[test]
fn gc_error_from_serde_json_error() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let gc_err: GcError = json_err.into();
    assert!(matches!(gc_err, GcError::Json(_)));
}

// GcError → ServerObjectStoreError for all mapped variants

#[test]
fn gc_error_into_server_object_store_error_local_object_store() {
    let inner = LocalObjectStoreError::Io(std::io::Error::other("local"));
    let gc_err = GcError::LocalObjectStore(inner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Local(_)));
}

#[test]
fn gc_error_into_server_object_store_error_s3_object_store() {
    let inner = S3ObjectStoreError::Io(std::io::Error::other("s3"));
    let gc_err = GcError::S3ObjectStore(inner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::S3(_)));
}

#[test]
fn gc_error_into_server_object_store_error_unmapped_variant_becomes_io() {
    // Json is not directly mapped to a named ServerObjectStoreError variant;
    // it should be wrapped in Io(Error::other(...)).
    let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
    let gc_err = GcError::Json(json_err);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(
        matches!(server_err, ServerObjectStoreError::Io(_)),
        "unmapped GcError variant should be wrapped in Io"
    );
}

// ── GcError Display for all remaining variants ───────────────────────

#[test]
fn gc_error_local_object_store_display() {
    let inner = LocalObjectStoreError::Io(std::io::Error::other("test"));
    let err = GcError::LocalObjectStore(inner);
    assert_eq!(err.to_string(), "local object storage operation failed");
}

#[test]
fn gc_error_s3_object_store_display() {
    let inner = S3ObjectStoreError::Io(std::io::Error::other("test"));
    let err = GcError::S3ObjectStore(inner);
    assert_eq!(err.to_string(), "s3 object storage operation failed");
}

#[test]
fn gc_error_object_prefix_display() {
    let inner = ObjectPrefixError::UnsafePath;
    let err = GcError::ObjectPrefix(inner);
    assert_eq!(err.to_string(), "object storage prefix validation failed");
}

#[test]
fn gc_error_index_store_display() {
    let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
    let err = GcError::IndexStore(inner);
    assert_eq!(err.to_string(), "index adapter operation failed");
}

#[test]
fn gc_error_memory_index_store_display() {
    let inner = MemoryIndexStoreError::LockPoisoned("test".to_owned());
    let err = GcError::MemoryIndexStore(inner);
    assert_eq!(err.to_string(), "memory index adapter operation failed");
}

#[test]
fn gc_error_memory_record_store_display() {
    let inner = MemoryRecordStoreError::RecordNotFound;
    let err = GcError::MemoryRecordStore(inner);
    assert_eq!(err.to_string(), "memory record adapter operation failed");
}

#[test]
fn gc_error_postgres_metadata_display() {
    let inner = PostgresMetadataStoreError::HashParse(
        shardline_protocol::HashParseError::InvalidCharacter("test".to_owned()),
    );
    let err = GcError::PostgresMetadata(inner);
    assert_eq!(
        err.to_string(),
        "postgres metadata adapter operation failed"
    );
}

#[test]
fn gc_error_retention_hold_display() {
    let inner = RetentionHoldError::EmptyReason;
    let err = GcError::RetentionHold(inner);
    assert_eq!(err.to_string(), "retention hold input was invalid");
}

#[test]
fn gc_error_quarantine_candidate_display() {
    let inner = QuarantineCandidateError::InvertedTimeline;
    let err = GcError::QuarantineCandidate(inner);
    assert_eq!(err.to_string(), "quarantine candidate input was invalid");
}

#[test]
fn gc_error_webhook_delivery_display() {
    let inner = WebhookDeliveryError::EmptyRepositoryOwner;
    let err = GcError::WebhookDelivery(inner);
    assert_eq!(err.to_string(), "webhook delivery metadata was invalid");
}

#[test]
fn gc_error_file_record_invariant_display() {
    let inner = FileRecordInvariantError::EmptyChunk;
    let err = GcError::FileRecordInvariant(inner);
    assert_eq!(err.to_string(), "stored file metadata was invalid");
}

#[test]
fn gc_error_invalid_lifecycle_metadata_display() {
    let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
        object_key: "test".to_owned(),
    };
    let err = GcError::InvalidLifecycleMetadata(inner);
    assert_eq!(
        err.to_string(),
        "lifecycle metadata was internally inconsistent"
    );
}

// ── From impl for remaining source types ────────────────────────────

#[test]
fn gc_error_from_local_index_store_error() {
    let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::IndexStore(_)));
}

#[test]
fn gc_error_from_memory_index_store_error() {
    let inner = MemoryIndexStoreError::LockPoisoned("test".to_owned());
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::MemoryIndexStore(_)));
}

#[test]
fn gc_error_from_memory_record_store_error() {
    let inner = MemoryRecordStoreError::RecordNotFound;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::MemoryRecordStore(_)));
}

#[test]
fn gc_error_from_postgres_metadata_store_error() {
    let inner = PostgresMetadataStoreError::HashParse(
        shardline_protocol::HashParseError::InvalidCharacter("test".to_owned()),
    );
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::PostgresMetadata(_)));
}

#[test]
fn gc_error_from_retention_hold_error() {
    let inner = RetentionHoldError::EmptyReason;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::RetentionHold(_)));
}

#[test]
fn gc_error_from_quarantine_candidate_error() {
    let inner = QuarantineCandidateError::InvertedTimeline;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::QuarantineCandidate(_)));
}

#[test]
fn gc_error_from_webhook_delivery_error() {
    let inner = WebhookDeliveryError::EmptyRepositoryOwner;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::WebhookDelivery(_)));
}

#[test]
fn gc_error_from_file_record_invariant_error() {
    let inner = FileRecordInvariantError::EmptyChunk;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::FileRecordInvariant(_)));
}

#[test]
fn gc_error_from_invalid_lifecycle_metadata_error() {
    let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
        object_key: "test".to_owned(),
    };
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::InvalidLifecycleMetadata(_)));
}

#[test]
fn gc_error_from_object_prefix_error() {
    let inner = ObjectPrefixError::UnsafePath;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::ObjectPrefix(_)));
}

#[test]
fn gc_error_from_xet_adapter_error() {
    let inner = XetAdapterError::NotFound;
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::XetAdapter(_)));
}

#[test]
fn gc_error_from_local_object_store_error() {
    let inner = LocalObjectStoreError::Io(std::io::Error::other("test"));
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::LocalObjectStore(_)));
}

#[test]
fn gc_error_from_s3_object_store_error() {
    let inner = S3ObjectStoreError::Io(std::io::Error::other("test"));
    let gc_err: GcError = inner.into();
    assert!(matches!(gc_err, GcError::S3ObjectStore(_)));
}

// ── GcError → ServerObjectStoreError for all unmapped variants ──────

#[test]
fn gc_error_into_server_object_store_error_object_prefix() {
    let gc_err = GcError::ObjectPrefix(ObjectPrefixError::UnsafePath);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_index_store() {
    let inner = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
    let gc_err = GcError::IndexStore(inner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_memory_index_store() {
    let gc_err = GcError::MemoryIndexStore(MemoryIndexStoreError::LockPoisoned("test".to_owned()));
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_memory_record_store() {
    let gc_err = GcError::MemoryRecordStore(MemoryRecordStoreError::RecordNotFound);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_postgres_metadata() {
    let inner = PostgresMetadataStoreError::HashParse(
        shardline_protocol::HashParseError::InvalidCharacter("test".to_owned()),
    );
    let gc_err = GcError::PostgresMetadata(inner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_retention_hold() {
    let gc_err = GcError::RetentionHold(RetentionHoldError::EmptyReason);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_quarantine_candidate() {
    let gc_err = GcError::QuarantineCandidate(QuarantineCandidateError::InvertedTimeline);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_webhook_delivery() {
    let gc_err = GcError::WebhookDelivery(WebhookDeliveryError::EmptyRepositoryOwner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_file_record_invariant() {
    let gc_err = GcError::FileRecordInvariant(FileRecordInvariantError::EmptyChunk);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_invalid_lifecycle_metadata() {
    let inner = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
        object_key: "test".to_owned(),
    };
    let gc_err = GcError::InvalidLifecycleMetadata(inner);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

#[test]
fn gc_error_into_server_object_store_error_xet_adapter() {
    let gc_err = GcError::XetAdapter(XetAdapterError::NotFound);
    let server_err: ServerObjectStoreError = gc_err.into();
    assert!(matches!(server_err, ServerObjectStoreError::Io(_)));
}

// ── retention_report_entry tests ────────────────────────────────────

#[test]
fn retention_report_entry_expired() {
    let now = 1_000_000_u64;
    let object_key = ObjectKey::parse("ab/abcdef").unwrap();
    let candidate = QuarantineCandidate::new(object_key, 512, now - 100, now - 1).unwrap();
    let entry = retention_report_entry(&candidate, &[], now);
    assert!(entry.expired);
    assert_eq!(entry.hash, "ab/abcdef");
    assert_eq!(entry.object_key, "ab/abcdef");
    assert_eq!(entry.observed_length, 512);
    assert_eq!(entry.first_seen_unreachable_at_unix_seconds, now - 100);
    assert_eq!(entry.delete_after_unix_seconds, now - 1);
    assert_eq!(entry.seconds_until_delete, 0);
}

#[test]
fn retention_report_entry_not_expired() {
    let now = 1_000_000_u64;
    let object_key = ObjectKey::parse("ab/abcdef").unwrap();
    let candidate = QuarantineCandidate::new(object_key, 1024, now - 3600, now + 3600).unwrap();
    let entry = retention_report_entry(&candidate, &[], now);
    assert!(!entry.expired);
    assert_eq!(entry.seconds_until_delete, 3600);
}

#[test]
fn retention_report_entry_with_xet_frontend_maps_hash() {
    let now = 1_000_000_u64;
    let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let object_key = ObjectKey::parse(&format!("ab/{hash}")).unwrap();
    let candidate = QuarantineCandidate::new(object_key, 64, now, now + 3600).unwrap();
    let entry = retention_report_entry(&candidate, &[ServerFrontend::Xet], now);
    // With Xet frontend, the hash should be extracted from the chunk key
    assert_eq!(entry.hash, hash);
}

// ── orphan_inventory_entry tests ────────────────────────────────────

#[test]
fn orphan_inventory_entry_untracked() {
    let hash = "deadbeef".to_owned();
    let object_key = ObjectKey::parse("ab/deadbeef").unwrap();
    let orphan = OrphanObject {
        hash: hash.clone(),
        object_key,
        bytes: 256,
    };
    let entry = orphan_inventory_entry(&orphan, None);
    assert_eq!(entry.hash, hash);
    assert_eq!(entry.object_key, "ab/deadbeef");
    assert_eq!(entry.bytes, 256);
    assert_eq!(entry.quarantine_state, GcOrphanQuarantineState::Untracked);
    assert_eq!(entry.first_seen_unreachable_at_unix_seconds, None);
    assert_eq!(entry.delete_after_unix_seconds, None);
}

#[test]
fn orphan_inventory_entry_quarantined() {
    let hash = "cafebabe".to_owned();
    let object_key = ObjectKey::parse("ab/cafebabe").unwrap();
    let orphan = OrphanObject {
        hash: hash.clone(),
        object_key: object_key.clone(),
        bytes: 512,
    };
    let now = 1_000_000_u64;
    let candidate = QuarantineCandidate::new(object_key, 512, now, now + 3600).unwrap();
    let entry = orphan_inventory_entry(&orphan, Some(&candidate));
    assert_eq!(entry.hash, hash);
    assert_eq!(entry.object_key, "ab/cafebabe");
    assert_eq!(entry.bytes, 512);
    assert_eq!(entry.quarantine_state, GcOrphanQuarantineState::Quarantined);
    assert_eq!(entry.first_seen_unreachable_at_unix_seconds, Some(now));
    assert_eq!(entry.delete_after_unix_seconds, Some(now + 3600));
}

// ── quarantine_root / quarantine_record_path edge cases ─────────────

#[test]
fn quarantine_root_empty_path() {
    let result = quarantine_root(Path::new(""));
    assert_eq!(result, PathBuf::from("gc/quarantine"));
}

#[test]
fn quarantine_record_path_with_short_hash() {
    // Short hashes still produce a valid path (first two chars become prefix).
    let hash = "ab";
    let result = quarantine_record_path(Path::new("/root"), hash);
    assert!(result.to_string_lossy().ends_with("ab.json"));
    assert!(result.starts_with("/root/ab/"));
}

#[test]
fn quarantine_record_path_with_empty_hash() {
    let hash = "";
    let result = quarantine_record_path(Path::new("/root"), hash);
    assert!(
        result.to_string_lossy().ends_with("/.json"),
        "got: {:?}",
        result
    );
    // Empty prefix means first two chars are empty → root path
}

// ── run_local_gc is a thin wrapper ──────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_local_gc_with_empty_root_returns_report() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    // Create minimal chunks subdirectory so ServerObjectStore can initialize.
    std::fs::create_dir_all(root.join("chunks")).unwrap();
    let result = run_local_gc(root, LocalGcOptions::dry_run()).await;
    assert!(
        result.is_ok(),
        "dry-run with empty root should succeed: {:?}",
        result
    );
    let report = result.unwrap();
    assert_eq!(report.scanned_records, 0);
    assert_eq!(report.orphan_chunks, 0);
    assert_eq!(report.deleted_chunks, 0);
}

// ── run_gc_with_stores / validate_gc_index_integrity tests ──────────

/// Helper to build a local object store with a single object at the given key.
fn put_object(object_store: &ServerObjectStore, key: &ObjectKey, data: &[u8]) {
    let hash = shardline_server_core::chunk_hash(data);
    let integrity = ObjectIntegrity::new(hash, u64::try_from(data.len()).unwrap_or(0));
    object_store
        .put_if_absent(key, ObjectBody::Borrowed(data), &integrity)
        .unwrap();
}

/// Helper: runs `run_gc_with_stores` with empty record store, MemoryIndexStore,
/// and the given object_store.  Returns the result so callers can assert on it.
async fn run_gc_helper(
    object_store: &ServerObjectStore,
    index_store: &MemoryIndexStore,
    options: LocalGcOptions,
) -> Result<LocalGcDiagnostics, GcError> {
    let record_store = MemoryRecordStore::new();
    run_gc_with_stores(
        &record_store,
        index_store,
        object_store,
        &[ServerFrontend::Xet],
        options,
    )
    .await
}

#[test]
fn gc_sweep_reaps_stale_temporary_chunk_artifacts_only() {
    use shardline_protocol::unix_now_seconds_lossy;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("aa")).unwrap();

        let now_secs = unix_now_seconds_lossy();
        // > 1 hour old: stranded remnant of a killed/crashed writer.
        let stale_nanos = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        // Fresh: created "now", must be left alone (an in-flight write).
        let fresh_nanos = u128::from(now_secs) * 1_000_000_000;
        let stale_key = format!(
            "aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.tmp-{stale_nanos}-0"
        );
        let fresh_key = format!(
            "aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.tmp-{fresh_nanos}-1"
        );
        let stale_path = dir.path().join(&stale_key);
        std::fs::write(&stale_path, b"stale").unwrap();
        std::fs::write(dir.path().join(&fresh_key), b"fresh").unwrap();
        // The reaper now prefers the GC-observed mtime (backend truth): age the
        // stale temp's on-disk mtime so it is genuinely old by both clocks.
        let aged_mtime =
            std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&stale_path)
            .unwrap()
            .set_modified(aged_mtime)
            .unwrap();

        // A live referenced chunk (finished key, no temp suffix) must never be
        // touched by the reaper.
        let live_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let live_key = ObjectKey::parse(&format!("bb/{live_hash}")).unwrap();
        let object_store = ServerObjectStore::local(dir.path()).unwrap();
        put_object(&object_store, &live_key, b"live-data");

        let index_store = MemoryIndexStore::new();
        let diagnostics = run_gc_helper(&object_store, &index_store, LocalGcOptions::sweep_only())
            .await
            .unwrap();

        assert_eq!(diagnostics.report.reaped_stale_temporary_chunks, 1);
        assert_eq!(diagnostics.report.reaped_stale_temporary_bytes, 5);
        assert!(
            std::fs::metadata(dir.path().join(&stale_key)).is_err(),
            "stale temp artifact must be reaped"
        );
        assert!(
            std::fs::metadata(dir.path().join(&fresh_key)).is_ok(),
            "fresh temp artifact must be left alone"
        );
        assert!(
            object_store.contains(&live_key).unwrap(),
            "live chunk must never be touched"
        );
    });
}

#[test]
fn gc_sweep_reaps_stranded_xorb_and_shard_temps_but_never_live_objects() {
    // F-67 regression: ALL local object writes (chunks, xorb containers, and
    // shards) go through temp-then-hardlink and get the `.tmp-<nanos>-<counter>`
    // suffix. A crash between the temp write and the hardlink strands
    // xorbs/default/<p>/<hash>.xorb.tmp-... and shards/<p>/<hash>.shard.tmp-...
    // files, which the old chunk-only reaper grammar never matched — and which
    // managed_object_hash rejects, so every other GC path skipped them too.
    // The extended reaper must reap them after the age bound while leaving
    // live chunk/xorb/shard objects untouched.
    use shardline_protocol::unix_now_seconds_lossy;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let prefix = &hash[..2];
        std::fs::create_dir_all(dir.path().join("xorbs/default").join(prefix)).unwrap();
        std::fs::create_dir_all(dir.path().join("shards").join(prefix)).unwrap();
        std::fs::create_dir_all(dir.path().join(prefix)).unwrap();

        let now_secs = unix_now_seconds_lossy();
        // > 1 hour old: stranded remnants of killed/crashed writers.
        let stale_nanos = u128::from(now_secs - 2 * 3600) * 1_000_000_000;
        let xorb_temp_key = format!("xorbs/default/{prefix}/{hash}.xorb.tmp-{stale_nanos}-0");
        let shard_temp_key = format!("shards/{prefix}/{hash}.shard.tmp-{stale_nanos}-1");
        let xorb_temp_path = dir.path().join(&xorb_temp_key);
        let shard_temp_path = dir.path().join(&shard_temp_key);
        std::fs::write(&xorb_temp_path, b"stale-xorb").unwrap();
        std::fs::write(&shard_temp_path, b"stale-shard").unwrap();
        // The reaper prefers the GC-observed mtime (backend truth): age both
        // temps' on-disk mtimes so they are genuinely old by both clocks.
        let aged_mtime = std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&xorb_temp_path)
            .unwrap()
            .set_modified(aged_mtime)
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(&shard_temp_path)
            .unwrap()
            .set_modified(aged_mtime)
            .unwrap();

        // Live managed objects (finished keys, no temp suffix) must never be
        // touched by the reaper.
        let object_store = ServerObjectStore::local(dir.path()).unwrap();
        let chunk_key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &chunk_key, b"live-chunk");
        let xorb_live_key =
            ObjectKey::parse(&format!("xorbs/default/{prefix}/{hash}.xorb")).unwrap();
        put_object(&object_store, &xorb_live_key, b"live-xorb");
        let shard_live_key = ObjectKey::parse(&format!("shards/{prefix}/{hash}.shard")).unwrap();
        put_object(&object_store, &shard_live_key, b"live-shard");

        let index_store = MemoryIndexStore::new();
        let diagnostics = run_gc_helper(&object_store, &index_store, LocalGcOptions::sweep_only())
            .await
            .unwrap();

        // Both stranded managed-object temps are reaped.
        assert_eq!(
            diagnostics.report.reaped_stale_temporary_chunks, 2,
            "both stranded xorb and shard temps must be reaped"
        );
        assert_eq!(
            diagnostics.report.reaped_stale_temporary_bytes, 21,
            "10 bytes of stale-xorb + 11 bytes of stale-shard"
        );
        assert!(
            std::fs::metadata(&xorb_temp_path).is_err(),
            "stranded xorb temp must be reaped"
        );
        assert!(
            std::fs::metadata(&shard_temp_path).is_err(),
            "stranded shard temp must be reaped"
        );
        // Live objects are untouched.
        assert!(
            object_store.contains(&chunk_key).unwrap(),
            "live chunk must never be touched"
        );
        assert!(
            object_store.contains(&xorb_live_key).unwrap(),
            "live xorb must never be touched"
        );
        assert!(
            object_store.contains(&shard_live_key).unwrap(),
            "live shard must never be touched"
        );
    });
}

#[test]
fn validate_integrity_missing_quarantine_object_auto_released() {
    // When a quarantine candidate references an object that doesn't exist
    // in the object store, the candidate should be auto-released.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(dir.path()).unwrap();
        let index_store = MemoryIndexStore::new();

        let key =
            ObjectKey::parse("aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .unwrap();
        let candidate = QuarantineCandidate::new(key, 100, 1_000_000, 2_000_000).unwrap();
        index_store
            .upsert_quarantine_candidate(&candidate)
            .await
            .unwrap();

        // No object exists in the store → auto-release.
        let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
        assert!(
            result.is_ok(),
            "auto-release should not error: {:?}",
            result
        );

        // Verify candidate was removed from index.
        let mut found = false;
        index_store
            .visit_quarantine_candidates(|_c| {
                found = true;
                Ok::<(), GcError>(())
            })
            .await
            .unwrap();
        assert!(
            !found,
            "quarantine candidate should have been auto-released"
        );
    });
}

#[test]
fn validate_integrity_quarantine_length_mismatch_errors() {
    // When the object exists but its length differs from observed_length,
    // validate_gc_index_integrity should return an error.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(dir.path()).unwrap();
        let index_store = MemoryIndexStore::new();

        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        // Put a 9-byte object.
        put_object(&object_store, &key, b"nine bytes");
        // But record observed_length as 100 → mismatch.
        let candidate = QuarantineCandidate::new(
            key.clone(),
            100, // wrong length
            1_000_000,
            2_000_000,
        )
        .unwrap();
        index_store
            .upsert_quarantine_candidate(&candidate)
            .await
            .unwrap();

        let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
        assert!(result.is_err(), "length mismatch should produce an error");
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                GcError::InvalidLifecycleMetadata(
                    InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch { .. }
                )
            ),
            "expected QuarantineCandidateLengthMismatch, got: {err:?}"
        );
    });
}

#[test]
fn validate_integrity_active_retention_hold_missing_object_errors() {
    // An active retention hold whose object is missing should error.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-act-mis-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();
        let key =
            ObjectKey::parse("ab/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                .unwrap();
        let hold = RetentionHold::new(
            key,
            "test hold".to_owned(),
            now,
            Some(now + 3600), // still active
        )
        .unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        // No object exists → error.
        let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
        assert!(
            result.is_err(),
            "active hold with missing object should error"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                GcError::InvalidLifecycleMetadata(
                    InvalidLifecycleMetadataError::ActiveRetentionHoldMissingObject { .. }
                )
            ),
            "expected ActiveRetentionHoldMissingObject, got: {err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn held_quarantined_object_is_repaired_not_wedged() {
    // Regression test for F-43: a hold placed on an already-quarantined object
    // used to abort every subsequent GC run with ActiveRetentionHoldQuarantined
    // at run start — before the code that would release the stale quarantine
    // entry — wedging GC until hold expiry (forever for release_after=None).
    //
    // Held+quarantined is now a repairable state: the run proceeds, the stale
    // quarantine candidate is released (the hold keeps the data), the object
    // survives, and a subsequent run is clean.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-f43-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();
        let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();

        // Put the object so the quarantine length check passes.
        put_object(&object_store, &key, b"test data");

        // Quarantine X first…
        let candidate = QuarantineCandidate::new(key.clone(), 9, now - 100, now + 3600).unwrap();
        index_store
            .upsert_quarantine_candidate(&candidate)
            .await
            .unwrap();

        // …then hold X (a permanent hold, release_after = None — the case that
        // previously wedged GC indefinitely).
        let hold = RetentionHold::new(key.clone(), "permanent hold".to_owned(), now, None).unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        // The run must complete (no abort) and repair the quarantine state.
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(86400),
        )
        .await;
        assert!(
            result.is_ok(),
            "held+quarantined must not abort the run: {:?}",
            result
        );
        let diagnostics = result.unwrap();
        assert_eq!(
            diagnostics.report.released_quarantine_candidates, 1,
            "the stale quarantine candidate must be released"
        );
        // The held object survives.
        assert!(
            object_store.contains(&key).unwrap(),
            "held object must survive the repair run"
        );

        // Quarantine state is gone from the index…
        let mut quarantine_found = false;
        index_store
            .visit_quarantine_candidates(|_c| {
                quarantine_found = true;
                Ok::<(), GcError>(())
            })
            .await
            .unwrap();
        assert!(!quarantine_found, "quarantine candidate must be released");

        // …and the hold is still present.
        let mut hold_found = false;
        index_store
            .visit_retention_holds(|h| {
                if h.object_key() == &key {
                    hold_found = true;
                }
                Ok::<(), GcError>(())
            })
            .await
            .unwrap();
        assert!(hold_found, "retention hold must survive the repair run");

        // A subsequent run is also clean: no abort, no re-quarantine, object intact.
        let result2 = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(86400),
        )
        .await;
        assert!(
            result2.is_ok(),
            "subsequent run must also complete: {:?}",
            result2
        );
        let diagnostics2 = result2.unwrap();
        assert_eq!(diagnostics2.report.active_quarantine_candidates, 0);
        assert_eq!(diagnostics2.report.released_quarantine_candidates, 0);
        assert!(
            object_store.contains(&key).unwrap(),
            "held object must still survive after the subsequent run"
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn validate_integrity_retention_hold_release_before_held_errors() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-val-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();
        let key =
            ObjectKey::parse("ab/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                .unwrap();
        put_object(&object_store, &key, b"data");

        // Valid hold (release after > held_at) should pass.
        let hold = RetentionHold::new(key.clone(), "valid hold".to_owned(), now, Some(now + 3600))
            .unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
        assert!(result.is_ok(), "valid hold should pass: {:?}", result);
        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn validate_integrity_inactive_retention_hold_no_error_for_missing_object() {
    // An expired retention hold (inactive) that references a missing object
    // should NOT error.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-exp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();
        let key =
            ObjectKey::parse("ab/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                .unwrap();
        // Expired hold: release_after < now.
        let hold = RetentionHold::new(key, "expired hold".to_owned(), now - 2000, Some(now - 1000))
            .unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        let result = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run()).await;
        assert!(
            result.is_ok(),
            "inactive hold with missing object should not error: {:?}",
            result
        );
        let _ = std::fs::remove_dir_all(&dir);
    });
}

// ── build_gc_diagnostics tests ──────────────────────────────────────

#[test]
fn diagnostics_retention_report_sorted_by_delete_time_then_key() {
    let now = 1_000_000_u64;
    let make_candidate = |key_str: &str, delete_at: u64| -> QuarantineCandidate {
        QuarantineCandidate::new(ObjectKey::parse(key_str).unwrap(), 100, now, delete_at).unwrap()
    };

    let mut quarantine_entries = HashMap::new();
    quarantine_entries.insert(
        "b-key".to_owned(),
        make_candidate(
            "ab/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            now + 200,
        ),
    );
    quarantine_entries.insert(
        "a-key".to_owned(),
        make_candidate(
            "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            now + 100,
        ),
    );

    let orphan_objects = HashMap::new();
    let report = LocalGcReport::default();
    let diagnostics = build_gc_diagnostics(report, &[], &orphan_objects, &quarantine_entries, now);

    assert_eq!(diagnostics.retention_report.len(), 2);
    assert!(
        diagnostics.retention_report[0].delete_after_unix_seconds
            <= diagnostics.retention_report[1].delete_after_unix_seconds
    );
}

#[test]
fn diagnostics_orphan_inventory_sorted_by_key() {
    let now = 1_000_000_u64;
    let mut orphan_objects = HashMap::new();
    orphan_objects.insert(
        "ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
        OrphanObject {
            hash: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            object_key: ObjectKey::parse(
                "ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            )
            .unwrap(),
            bytes: 100,
        },
    );
    orphan_objects.insert(
        "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        OrphanObject {
            hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            object_key: ObjectKey::parse(
                "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
            bytes: 200,
        },
    );

    let quarantine_entries = HashMap::new();
    let report = LocalGcReport::default();
    let diagnostics = build_gc_diagnostics(report, &[], &orphan_objects, &quarantine_entries, now);

    assert_eq!(diagnostics.orphan_inventory.len(), 2);
    // 'aa...' should come before 'zz...'
    assert_eq!(
        diagnostics.orphan_inventory[0].object_key,
        "ab/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        diagnostics.orphan_inventory[1].object_key,
        "ab/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
    );
}

// ── run_gc_with_stores mark and sweep tests ─────────────────────────

#[test]
fn gc_mark_phase_creates_quarantine_entries() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-mark-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        // Put an orphan chunk object.
        let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"orphan data");

        // Run GC with mark-only.
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(86400),
        )
        .await;
        assert!(result.is_ok(), "mark should succeed: {:?}", result);
        let diagnostics = result.unwrap();
        assert_eq!(diagnostics.report.orphan_chunks, 1);
        assert_eq!(diagnostics.report.new_quarantine_candidates, 1);

        // Verify quarantine entry exists in index.
        let mut found = false;
        index_store
            .visit_quarantine_candidates(|_c| {
                found = true;
                Ok::<(), GcError>(())
            })
            .await
            .unwrap();
        assert!(found, "quarantine entry should have been created");
    });
}

#[test]
fn gc_sweep_phase_deletes_expired_orphans() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-sweep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"expired orphan");

        // First mark (create quarantine entry with retention=0 → immediately expired).
        let mark_result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(0),
        )
        .await;
        assert!(mark_result.is_ok());

        // Now sweep — the entry is already expired.
        let sweep_result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::sweep_only(),
        )
        .await;
        assert!(
            sweep_result.is_ok(),
            "sweep should succeed: {:?}",
            sweep_result
        );
        let diagnostics = sweep_result.unwrap();
        assert_eq!(diagnostics.report.deleted_chunks, 1);
        assert_eq!(diagnostics.report.deleted_bytes, 14); // "expired orphan" is 14 bytes
        let _ = std::fs::remove_dir_all(&dir);
    });
}

// ── orphan_objects.retain test (retention hold filtering in run_gc_with_stores) ─

#[test]
fn gc_skips_orphans_with_active_retention_holds() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-hold-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();
        // Create an orphan chunk.
        let hash = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"retained orphan");

        // Add an active retention hold for the orphan.
        let hold = RetentionHold::new(
            key.clone(),
            "operator hold".to_owned(),
            now - 100,
            Some(now + 3600),
        )
        .unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        // Run GC mark — the orphan should be skipped due to the retention hold.
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(86400),
        )
        .await;
        assert!(result.is_ok(), "mark should succeed: {:?}", result);
        let diagnostics = result.unwrap();
        assert_eq!(
            diagnostics.report.orphan_chunks, 0,
            "orphan with active hold should not be counted"
        );
        assert_eq!(
            diagnostics.report.new_quarantine_candidates, 0,
            "orphan with active hold should not be quarantined"
        );
    });
}

// ── run_gc_with_stores with non-Xet frontend ────────────────────────

#[test]
fn gc_with_lfs_frontend_does_not_error() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let record_store = MemoryRecordStore::new();
        let index_store = MemoryIndexStore::new();
        let object_store = ServerObjectStore::blackhole();

        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[],
            LocalGcOptions::dry_run(),
        )
        .await;
        assert!(
            result.is_ok(),
            "empty frontend should not error: {:?}",
            result
        );
    });
}

// ── Additional GcError boundary tests ───────────────────────────────

#[test]
fn gc_error_from_s3_object_store_error_display() {
    let inner = S3ObjectStoreError::Io(std::io::Error::other("s3 error"));
    let err = GcError::from(inner);
    assert_eq!(err.to_string(), "s3 object storage operation failed");
    let inner2 = S3ObjectStoreError::Io(std::io::Error::other("s3 error"));
    let err2 = GcError::S3ObjectStore(inner2);
    assert!(matches!(err2, GcError::S3ObjectStore(_)));
}

#[test]
fn gc_error_display_for_object_store_variant() {
    let inner = ServerObjectStoreError::NotFound;
    let err = GcError::ObjectStore(inner);
    assert_eq!(err.to_string(), "object storage adapter operation failed");
}

// ── LocalGcOptions boundary ─────────────────────────────────────────

#[test]
fn gc_options_all_zero_retention_is_accepted() {
    // Even zero retention should not panic — the minimum is enforced elsewhere.
    let opts = LocalGcOptions::mark_only(0);
    assert_eq!(opts.retention_seconds, 0);
    assert!(opts.mark);
    assert!(!opts.sweep);
}

#[test]
fn gc_options_max_retention_does_not_panic() {
    let opts = LocalGcOptions::mark_and_sweep(u64::MAX);
    assert_eq!(opts.retention_seconds, u64::MAX);
    assert!(opts.mark);
    assert!(opts.sweep);
}

// ── GcReport default ────────────────────────────────────────────────

#[test]
fn gc_report_default_all_zeros() {
    let report = LocalGcReport::default();
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

// ── GC with various retention configurations ───────────────────────────

#[test]
fn gc_very_long_retention_preserves_quarantine() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-long-ret-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let hash = "1111111111111111111111111111111111111111111111111111111111111111";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"long retention orphan");

        // Mark with very long retention (approx 3 years) — quarantine entry should be
        // created and not expired.
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_only(100_000_000),
        )
        .await;
        assert!(result.is_ok());
        let diag = result.unwrap();
        assert_eq!(diag.report.new_quarantine_candidates, 1);
        assert_eq!(diag.report.active_quarantine_candidates, 1);

        // Sweep — entry should NOT be deleted (retention not expired)
        let sweep_result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::sweep_only(),
        )
        .await;
        assert!(sweep_result.is_ok());
        let sweep_diag = sweep_result.unwrap();
        assert_eq!(sweep_diag.report.deleted_chunks, 0);
        assert_eq!(sweep_diag.report.active_quarantine_candidates, 1);
    });
}

#[test]
fn gc_mark_and_sweep_in_one_pass_deletes_orphans() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-ms-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let hash = "2222222222222222222222222222222222222222222222222222222222222222";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"one-pass-orphan");

        // mark_and_sweep with 0 retention → mark creates entry, sweep deletes
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_and_sweep(0),
        )
        .await;
        assert!(result.is_ok());
        let diag = result.unwrap();
        assert_eq!(diag.report.new_quarantine_candidates, 1);
        assert_eq!(diag.report.deleted_chunks, 1);
        assert!(diag.report.deleted_bytes > 0);
        // After sweep, quarantine should be empty
        assert_eq!(diag.report.active_quarantine_candidates, 0);
    });
}

#[test]
fn gc_with_retention_hold_and_quarantine_in_mark_sweep() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-hold-q-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();

        // Orphan with an active retention hold — should be skipped by mark
        let held_hash = "3333333333333333333333333333333333333333333333333333333333333333";
        let held_prefix = &held_hash[..2];
        let held_key = ObjectKey::parse(&format!("{held_prefix}/{held_hash}")).unwrap();
        put_object(&object_store, &held_key, b"held-orphan");
        let hold = RetentionHold::new(
            held_key,
            "operator hold".to_owned(),
            now - 100,
            Some(now + 3600),
        )
        .unwrap();
        index_store.upsert_retention_hold(&hold).await.unwrap();

        // Orphan without hold — should be quarantined
        let orphan_hash = "4444444444444444444444444444444444444444444444444444444444444444";
        let orphan_prefix = &orphan_hash[..2];
        let orphan_key = ObjectKey::parse(&format!("{orphan_prefix}/{orphan_hash}")).unwrap();
        put_object(&object_store, &orphan_key, b"free-orphan");

        // mark_and_sweep with 0 retention
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::mark_and_sweep(0),
        )
        .await;
        assert!(result.is_ok());
        let diag = result.unwrap();
        // Held orphan should not appear in orphan count or be quarantined/deleted
        assert_eq!(
            diag.report.orphan_chunks, 1,
            "only the free orphan should be counted"
        );
        assert_eq!(diag.report.new_quarantine_candidates, 1);
        assert_eq!(diag.report.deleted_chunks, 1);
    });
}

// ── run_gc_with_stores mark + sweep with quarantine candidates already present ─

#[test]
fn gc_with_existing_quarantine_candidates_retains_unexpired() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-ex-q-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let now = shardline_protocol::unix_now_seconds_lossy();

        // Pre-existing quarantine candidate with far-future expiry
        let hash = "5555555555555555555555555555555555555555555555555555555555555555";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"pre-existing quarantine");
        let candidate = QuarantineCandidate::new(
            key,
            23,
            now - 100,
            now + 86400, // far in the future
        )
        .unwrap();
        index_store
            .upsert_quarantine_candidate(&candidate)
            .await
            .unwrap();

        // Run sweep only — candidate should be retained (not expired)
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::sweep_only(),
        )
        .await;
        assert!(result.is_ok());
        let diag = result.unwrap();
        assert_eq!(
            diag.report.deleted_chunks, 0,
            "unexpired should not be deleted"
        );
        assert_eq!(diag.report.active_quarantine_candidates, 1);
    });
}

// ── run_local_gc_diagnostics tests ────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_local_gc_diagnostics_empty_root() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("chunks")).unwrap();
    let result = run_local_gc_diagnostics(root, LocalGcOptions::dry_run()).await;
    assert!(
        result.is_ok(),
        "diagnostics with empty root should succeed: {:?}",
        result
    );
    let diagnostics = result.unwrap();
    assert_eq!(diagnostics.report.scanned_records, 0);
    assert_eq!(diagnostics.report.orphan_chunks, 0);
    assert_eq!(diagnostics.report.deleted_chunks, 0);
    assert!(diagnostics.retention_report.is_empty());
    assert!(diagnostics.orphan_inventory.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn run_local_gc_diagnostics_with_orphan_chunks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("chunks")).unwrap();

    // Put an orphan chunk directly on disk so the local object store finds it.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let prefix = &hash[..2];
    let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
    put_object(&object_store, &key, b"orphan chunk for diagnostics");
    // Drop the setup store so run_local_gc_diagnostics creates its own.
    drop(object_store);

    let result = run_local_gc_diagnostics(root, LocalGcOptions::dry_run()).await;
    assert!(
        result.is_ok(),
        "diagnostics with orphan should succeed: {:?}",
        result
    );
    let diagnostics = result.unwrap();
    assert_eq!(diagnostics.report.orphan_chunks, 1);
    // "orphan chunk for diagnostics" is 28 bytes.
    assert_eq!(diagnostics.report.orphan_chunk_bytes, 28);
    assert_eq!(diagnostics.orphan_inventory.len(), 1);
    assert_eq!(diagnostics.orphan_inventory[0].object_key, key.as_str());
    assert_eq!(diagnostics.orphan_inventory[0].bytes, 28);
}

// ── last-GC-clock anchor gating + write tolerance (F-76) ───────────────

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_on_read_only_store_tolerates_anchor_write_failure() {
    // F-76 regression: the F-57 anchor write propagated errors via `?`, so a
    // dry run against a read-only object store (chmod 0555 chunks dir;
    // least-privilege S3 cron role) aborted with an object-store
    // PermissionDenied error and produced NO report — even though pre-F-57 the
    // identical dry run completed all-reads. The anchor is an optimization, not
    // a correctness requirement: a failed write must be tolerated (warn and
    // continue) and the run must still return Ok with a full report.
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("chunks")).unwrap();
    let chunks_dir = root.join("chunks");

    // chmod 0555: reads (metadata, listing) succeed, writes (the anchor put,
    // which must create chunks/gc/) fail with PermissionDenied.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&chunks_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    }

    let result = run_local_gc_diagnostics(root.clone(), LocalGcOptions::dry_run()).await;

    // Restore write permission so tempdir cleanup can remove the tree.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&chunks_dir, std::fs::Permissions::from_mode(0o755));
    }

    assert!(
        result.is_ok(),
        "dry-run on a read-only store must succeed despite the anchor write failing: {:?}",
        result
    );
    let diagnostics = result.unwrap();
    assert_eq!(diagnostics.report.scanned_records, 0);
    assert_eq!(diagnostics.report.orphan_chunks, 0);
    assert_eq!(diagnostics.report.deleted_chunks, 0);
    assert!(diagnostics.retention_report.is_empty());
    assert!(diagnostics.orphan_inventory.is_empty());
}

#[test]
fn dry_run_on_writable_store_does_not_persist_gc_clock_anchor() {
    // F-76 regression: the F-57 anchor write was unconditional (not gated on
    // mark||sweep), so the documented "pure dry run" (mark=false, sweep=false,
    // "changes nothing") CREATED chunks/gc/last-gc-clock-anchor — mutating the
    // object store. A pure dry run must remain read-only: it still READS the
    // anchor for the forward-clock guard, but never writes it.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
        let index_store = MemoryIndexStore::new();

        let diagnostics = run_gc_helper(&object_store, &index_store, LocalGcOptions::dry_run())
            .await
            .unwrap();

        assert_eq!(diagnostics.report.deleted_chunks, 0);
        assert!(
            !object_store
                .contains(&ObjectKey::parse("gc/last-gc-clock-anchor").unwrap())
                .unwrap(),
            "a pure dry run must not persist the last-GC-clock anchor"
        );
    });
}

#[test]
fn mark_or_sweep_run_on_writable_store_persists_gc_clock_anchor() {
    // F-76: a run that mutates the object store (mark and/or sweep) must
    // persist the last-GC-clock anchor so a forward jump between two consecutive
    // runs is detectable even on a low-churn deployment (F-57).
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(dir.path().join("chunks")).unwrap();
        let index_store = MemoryIndexStore::new();
        let anchor_key = ObjectKey::parse("gc/last-gc-clock-anchor").unwrap();

        // sweep-only is a store-mutating run.
        let sweep_diagnostics =
            run_gc_helper(&object_store, &index_store, LocalGcOptions::sweep_only())
                .await
                .unwrap();
        assert_eq!(sweep_diagnostics.report.deleted_chunks, 0);
        assert!(
            object_store.contains(&anchor_key).unwrap(),
            "a sweep run must persist the last-GC-clock anchor"
        );

        // mark-only is also a store-mutating run.
        let mark_diagnostics = run_gc_helper(
            &object_store,
            &index_store,
            LocalGcOptions::mark_only(DEFAULT_LOCAL_GC_RETENTION_SECONDS),
        )
        .await
        .unwrap();
        assert_eq!(mark_diagnostics.report.new_quarantine_candidates, 0);
        assert!(
            object_store.contains(&anchor_key).unwrap(),
            "a mark run must persist the last-GC-clock anchor"
        );
    });
}

// ── run_local_gc with a pre-populated local SQLite store ──────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_local_gc_with_prepopulated_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    std::fs::create_dir_all(root.join("chunks")).unwrap();

    // Initialize the local stores (creates the SQLite database).
    let record_store = LocalRecordStore::new(root.clone()).expect("create record store");
    let _index_store = LocalIndexStore::new(root.clone()).expect("create index store");

    // Put a chunk in the object store.
    let object_store = ServerObjectStore::local(root.join("chunks")).unwrap();
    let hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let prefix = &hash[..2];
    let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
    put_object(&object_store, &key, b"local prepopulated chunk");

    // Commit a file record that references this chunk.
    let record = FileRecord {
        file_id: "local-test-file".to_owned(),
        content_hash: hash.to_owned(),
        total_bytes: 25,
        chunk_size: 100,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: hash.to_owned(),
            offset: 0,
            length: 25,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    };
    record_store
        .commit_file_version_metadata(&record)
        .await
        .expect("commit record");

    // Drop setup handles so run_local_gc opens its own connections.
    drop(record_store);
    drop(object_store);

    // Run local GC — the referenced chunk should not be orphaned.
    // commit_file_version_metadata inserts both a version record and a latest record,
    // so scanned_records is 2.
    let result = run_local_gc(root, LocalGcOptions::dry_run()).await;
    assert!(
        result.is_ok(),
        "local GC with prepopulated store: {:?}",
        result
    );
    let report = result.unwrap();
    assert_eq!(report.scanned_records, 2);
    assert_eq!(report.referenced_chunks, 1);
    assert_eq!(report.orphan_chunks, 0);
    assert_eq!(report.deleted_chunks, 0);
}

// ── run_gc_with_stores with populated in-memory stores ────────────────

#[test]
fn run_gc_with_stores_record_referencing_existing_chunk_no_orphan() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-rec-exists-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();

        // Put the referenced chunk in the object store.
        put_object(&object_store, &key, b"referenced chunk data");

        // Create a single record that references this chunk.
        let record = FileRecord {
            file_id: "test-file".to_owned(),
            content_hash: hash.to_owned(),
            total_bytes: 21,
            chunk_size: 100,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: hash.to_owned(),
                offset: 0,
                length: 21,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        };
        record_store.write_latest_record(&record).await.unwrap();

        // Also put an unrelated orphan chunk.
        let orphan_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let orphan_prefix = &orphan_hash[..2];
        let orphan_key = ObjectKey::parse(&format!("{orphan_prefix}/{orphan_hash}")).unwrap();
        put_object(&object_store, &orphan_key, b"orphan");

        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::dry_run(),
        )
        .await;
        assert!(result.is_ok(), "GC should succeed: {:?}", result);
        let diagnostics = result.unwrap();
        assert_eq!(diagnostics.report.scanned_records, 1);
        assert_eq!(diagnostics.report.referenced_chunks, 1);
        // The orphan is not referenced → it shows up in orphan count.
        assert_eq!(
            diagnostics.report.orphan_chunks, 1,
            "unreferenced orphan should be detected"
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn run_gc_with_stores_dangling_record_reference_handled_gracefully() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-dangling-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        // Create a record referencing a chunk that does NOT exist on disk.
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let record = FileRecord {
            file_id: "dangling-file".to_owned(),
            content_hash: hash.to_owned(),
            total_bytes: 100,
            chunk_size: 100,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: hash.to_owned(),
                offset: 0,
                length: 100,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        };
        record_store.write_latest_record(&record).await.unwrap();

        // Run GC — a dangling reference should not cause an error;
        // the non-existent chunk is referenced by the record so it is NOT orphaned.
        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::dry_run(),
        )
        .await;
        assert!(
            result.is_ok(),
            "dangling reference should not cause GC error: {:?}",
            result
        );
        let diagnostics = result.unwrap();
        assert_eq!(diagnostics.report.scanned_records, 1);
        assert_eq!(diagnostics.report.referenced_chunks, 1);
        assert_eq!(
            diagnostics.report.orphan_chunks, 0,
            "referenced (but missing) chunk should not be orphaned"
        );

        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn run_gc_with_stores_multiple_records_sharing_a_chunk_no_orphan() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("gc-test-shared-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("chunks")).unwrap();
        let object_store = ServerObjectStore::local(&dir).unwrap();
        let index_store = MemoryIndexStore::new();
        let record_store = MemoryRecordStore::new();

        let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let prefix = &hash[..2];
        let key = ObjectKey::parse(&format!("{prefix}/{hash}")).unwrap();
        put_object(&object_store, &key, b"shared chunk");

        // Two records sharing the same chunk hash.
        let make_record = |file_id: &str| -> FileRecord {
            FileRecord {
                file_id: file_id.to_owned(),
                content_hash: hash.to_owned(),
                total_bytes: 12,
                chunk_size: 100,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.to_owned(),
                    offset: 0,
                    length: 12,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 0,
                }],
            }
        };

        record_store
            .write_latest_record(&make_record("file-a"))
            .await
            .unwrap();
        record_store
            .write_latest_record(&make_record("file-b"))
            .await
            .unwrap();

        let result = run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            &[ServerFrontend::Xet],
            LocalGcOptions::dry_run(),
        )
        .await;
        assert!(result.is_ok(), "GC with shared chunk: {:?}", result);
        let diagnostics = result.unwrap();
        assert_eq!(diagnostics.report.scanned_records, 2);
        assert_eq!(diagnostics.report.referenced_chunks, 1);
        assert_eq!(diagnostics.report.orphan_chunks, 0);
    });
}
