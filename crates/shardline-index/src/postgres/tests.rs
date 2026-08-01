#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use
)]

use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};

use super::PostgresRecordKind;
use super::index_store::PostgresFileReconstructionRecord;
use super::record_store::record_locator;
use super::types::{i64_to_u64, u64_to_i64};
use crate::{
    FileId, FileReconstruction, FileRecord, ReconstructionTerm, RepositoryRecordScope,
    StoredObjectId, XorbId,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_record_scope_key as shared_repository_record_scope_key,
        repository_scope_key as shared_repository_scope_key,
    },
    xet_hash_hex_string,
};

#[test]
fn postgres_reconstruction_record_roundtrips_domain_terms() {
    let hash = ShardlineHash::from_bytes([11; 32]);
    let range = ChunkRange::new(1, 4);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(XorbId::new(hash), range, 256)]);

    let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
    let restored = record.into_domain();

    assert!(matches!(restored, Ok(ref restored) if restored == &reconstruction));
}

#[test]
fn postgres_record_keys_distinguish_scope_file_and_kind_without_parsing() {
    let first_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team:a", "asset", Some("main"));
    let second_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "a:asset", Some("main"));
    assert!(first_scope.is_ok());
    assert!(second_scope.is_ok());
    let (Ok(first_scope), Ok(second_scope)) = (first_scope, second_scope) else {
        return;
    };

    let first_key = shared_repository_scope_key(Some(&first_scope));
    let second_key = shared_repository_scope_key(Some(&second_scope));

    assert_ne!(first_key, second_key);
    assert_ne!(
        shared_record_key(
            PostgresRecordKind::Latest.as_str(),
            &first_key,
            "file",
            None
        ),
        shared_record_key(
            PostgresRecordKind::Version.as_str(),
            &first_key,
            "file",
            Some("a".repeat(64).as_str())
        )
    );
}

#[test]
fn postgres_latest_locator_ignores_content_hash_for_stable_head_keys() {
    let scope = RepositoryScope::new(RepositoryProvider::GitLab, "team", "assets", None);
    assert!(scope.is_ok());
    let Ok(scope) = scope else {
        return;
    };
    let first = file_record(scope.clone(), "a");
    let second = file_record(scope, "b");
    let first_key = record_locator(PostgresRecordKind::Latest, &first, None);
    let second_key = record_locator(PostgresRecordKind::Latest, &second, None);

    assert_eq!(first_key, second_key);
}

#[test]
fn postgres_repository_scope_key_prefix_matches_all_repository_revisions_only() {
    let repository = RepositoryRecordScope::new(RepositoryProvider::GitHub, "team", "assets");
    let revisionless = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None);
    let revisioned =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
    let other = RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"));
    assert!(revisionless.is_ok());
    assert!(revisioned.is_ok());
    assert!(other.is_ok());
    let (Ok(revisionless), Ok(revisioned), Ok(other)) = (revisionless, revisioned, other) else {
        return;
    };

    let repository_key = shared_repository_record_scope_key(&repository);
    let revisionless_key = shared_repository_scope_key(Some(&revisionless));
    let revisioned_key = shared_repository_scope_key(Some(&revisioned));
    let other_key = shared_repository_scope_key(Some(&other));

    assert_eq!(repository_key, revisionless_key);
    assert!(revisioned_key.starts_with(&repository_key));
    assert!(!other_key.starts_with(&repository_key));
}

#[test]
fn postgres_lifecycle_migrations_reject_inverted_timelines() {
    let metadata_migration = include_str!("../../migrations/20260417000000_metadata_store.up.sql");
    let retention_migration =
        include_str!("../../migrations/20260417010000_retention_holds.up.sql");

    assert!(
        metadata_migration.contains(
            "CHECK (delete_after_unix_seconds >= first_seen_unreachable_at_unix_seconds)"
        )
    );
    assert!(retention_migration.contains("release_after_unix_seconds >= held_at_unix_seconds"));
}

// ------------------------------------------------------------------
// PostgresFileReconstructionRecord through public API
// ------------------------------------------------------------------
#[test]
fn postgres_file_reconstruction_record_empty_terms() {
    let reconstruction = FileReconstruction::new(vec![]);
    let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
    let result = record.into_domain().expect("empty terms is valid");
    assert!(result.terms().is_empty());
}

#[test]
fn postgres_file_reconstruction_record_multiple_terms() {
    let hash_a = ShardlineHash::from_bytes([1; 32]);
    let hash_b = ShardlineHash::from_bytes([2; 32]);
    let range_a = ChunkRange::new(0, 1).unwrap();
    let range_b = ChunkRange::new(1, 3).unwrap();
    let reconstruction = FileReconstruction::new(vec![
        ReconstructionTerm::new(StoredObjectId::new(hash_a), range_a, 64),
        ReconstructionTerm::new(StoredObjectId::new(hash_b), range_b, 128),
    ]);
    let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
    let restored = record.into_domain().expect("valid reconstruction");
    assert_eq!(restored.terms().len(), 2);
    assert_eq!(restored.terms()[0].object_id(), StoredObjectId::new(hash_a));
    assert_eq!(restored.terms()[1].object_id(), StoredObjectId::new(hash_b));
}

// ------------------------------------------------------------------
// record_locator (from record_store.rs) — pure function, no pool needed
// ------------------------------------------------------------------
#[test]
fn postgres_record_locator_version_includes_content_hash_in_key() {
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", None).unwrap();
    let record = file_record(scope, "content");
    let locator = record_locator(
        PostgresRecordKind::Version,
        &record,
        Some(record.content_hash.clone()),
    );

    // Use the public accessor methods
    assert_eq!(locator.content_hash(), Some(record.content_hash.as_str()));
    assert_ne!(locator.record_key(), locator.file_id());
}

#[test]
fn postgres_record_locator_latest_has_no_content_hash() {
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", None).unwrap();
    let record = file_record(scope, "content");
    let locator = record_locator(PostgresRecordKind::Latest, &record, None);

    assert!(locator.content_hash().is_none());
    assert_eq!(locator.file_id(), record.file_id);
}

#[test]
fn postgres_record_locator_keys_differ_by_kind() {
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", None).unwrap();
    let record = file_record(scope, "content");
    let latest = record_locator(PostgresRecordKind::Latest, &record, None);
    let version = record_locator(
        PostgresRecordKind::Version,
        &record,
        Some(record.content_hash.clone()),
    );

    assert_ne!(latest.record_key(), version.record_key());
    assert_eq!(latest.file_id(), version.file_id());
}

// ------------------------------------------------------------------
// u64_to_i64 / i64_to_u64 tests (re-exercise the functions)
// ------------------------------------------------------------------
#[test]
fn postgres_conversion_functions_roundtrip() {
    let original: u64 = 42;
    let as_i64 = u64_to_i64(original).unwrap();
    let back = i64_to_u64(as_i64).unwrap();
    assert_eq!(original, back);
}

#[test]
fn postgres_u64_to_i64_max() {
    assert!(matches!(u64_to_i64(i64::MAX as u64), Ok(v) if v == i64::MAX));
    assert!(u64_to_i64(u64::MAX).is_err());
}

#[test]
fn postgres_i64_to_u64_min() {
    assert!(matches!(i64_to_u64(0), Ok(v) if v == 0));
    assert!(i64_to_u64(-1).is_err());
    assert!(i64_to_u64(i64::MIN).is_err());
}

fn file_record(scope: RepositoryScope, content_seed: &str) -> FileRecord {
    let hash = ShardlineHash::from_bytes([12; 32]);
    let file_id = xet_hash_hex_string(FileId::new(hash).hash());
    FileRecord {
        file_id,
        content_hash: content_seed.repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: crate::StorageRepresentation::FixedChunkV1,
        repository_scope: Some(scope),
        chunks: Vec::new(),
    }
}
