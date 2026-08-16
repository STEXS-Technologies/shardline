use std::{collections::HashMap, time::Duration};

use shardline_index::{FileRecord, FileRecordInvariantError, RecordTraversal, StoredRecord};
use shardline_protocol::{RepositoryScope, TokenClaimsError};
use shardline_server_core::{
    OpsRecordStore, provider_directory, validate_content_hash, validate_identifier,
};

use super::{
    IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReconstructionPlanDetail,
    IndexRebuildReport, RebuildError, push_issue,
};
use shardline_server_core::parse_stored_file_record_bytes;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct RebuildKey {
    provider: Option<&'static str>,
    owner: Option<String>,
    name: Option<String>,
    revision: Option<String>,
    file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionCandidate<Locator> {
    pub(super) record: FileRecord,
    pub(super) locator: Locator,
    pub(super) modified_since_epoch: Duration,
}

pub(super) fn collect_candidate<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: StoredRecord<RecordAdapter::Locator>,
    candidates: &mut HashMap<RebuildKey, VersionCandidate<RecordAdapter::Locator>>,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    RecordAdapter: OpsRecordStore,
    RecordAdapter::Error: Into<RebuildError>,
{
    let StoredRecord {
        locator: path,
        bytes,
        modified_since_epoch,
    } = entry;
    let location = record_store.locator_display(&path);
    let record = match parse_stored_file_record_bytes(&bytes) {
        Ok(record) => record,
        Err(shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            ..
        }) => {
            push_issue(
                report,
                IndexRebuildIssueKind::OversizedVersionRecordMetadata,
                location,
                IndexRebuildIssueDetail::OversizedVersionRecordMetadata,
            )?;
            report
                .preserved_latest_records_unreadable_version
                .push(record_store.locator_display(&path));
            return Ok(());
        }
        Err(shardline_server_core::ParseStoredFileRecordError::Json(_)) => {
            push_issue(
                report,
                IndexRebuildIssueKind::InvalidVersionRecordJson,
                location,
                IndexRebuildIssueDetail::RecordJsonInvalid,
            )?;
            report
                .preserved_latest_records_unreadable_version
                .push(record_store.locator_display(&path));
            return Ok(());
        }
    };

    if validate_identifier(&record.file_id).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionFileId,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidFileId {
                file_id: record.file_id,
            },
        )?;
        report
            .preserved_latest_records_unreadable_version
            .push(record_store.locator_display(&path));
        return Ok(());
    }

    if validate_content_hash(&record.content_hash).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionContentHash,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidContentHash {
                content_hash: record.content_hash,
            },
        )?;
        report
            .preserved_latest_records_unreadable_version
            .push(record_store.locator_display(&path));
        return Ok(());
    }

    if validate_repository_scope(record.repository_scope.as_ref()).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionRepositoryScope,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidRepositoryScope,
        )?;
        report
            .preserved_latest_records_unreadable_version
            .push(record_store.locator_display(&path));
        return Ok(());
    }

    let expected_path = RecordTraversal::version_record_locator(record_store, &record);
    if expected_path != path {
        push_issue(
            report,
            IndexRebuildIssueKind::VersionPathMismatch,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::VersionPathMismatch {
                expected_locator: record_store.locator_display(&expected_path),
            },
        )?;
        report
            .preserved_latest_records_unreadable_version
            .push(record_store.locator_display(&path));
        return Ok(());
    }

    if let Err(error) = record.validate_reconstruction_plan() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionReconstructionPlan,
            record_store.locator_display(&path),
            reconstruction_plan_error_detail(&error),
        )?;
        report
            .preserved_latest_records_unreadable_version
            .push(record_store.locator_display(&path));
        return Ok(());
    }

    let candidate = VersionCandidate {
        record: record.clone(),
        locator: path,
        modified_since_epoch,
    };
    let key = rebuild_key(&record);
    match candidates.get_mut(&key) {
        Some(existing) if candidate_is_newer(&candidate, existing) => {
            *existing = candidate;
        }
        None => {
            candidates.insert(key, candidate);
        }
        Some(_) => {}
    }

    Ok(())
}

fn rebuild_key(record: &FileRecord) -> RebuildKey {
    record.repository_scope.as_ref().map_or_else(
        || RebuildKey {
            provider: None,
            owner: None,
            name: None,
            revision: None,
            file_id: record.file_id.clone(),
        },
        |repository_scope| RebuildKey {
            provider: Some(provider_directory(repository_scope.provider())),
            owner: Some(repository_scope.owner().to_owned()),
            name: Some(repository_scope.name().to_owned()),
            revision: repository_scope.revision().map(ToOwned::to_owned),
            file_id: record.file_id.clone(),
        },
    )
}

fn candidate_is_newer<Locator>(
    candidate: &VersionCandidate<Locator>,
    existing: &VersionCandidate<Locator>,
) -> bool
where
    Locator: Ord,
{
    candidate.modified_since_epoch > existing.modified_since_epoch
        || (candidate.modified_since_epoch == existing.modified_since_epoch
            && candidate.record.content_hash > existing.record.content_hash)
        || (candidate.modified_since_epoch == existing.modified_since_epoch
            && candidate.record.content_hash == existing.record.content_hash
            && candidate.locator > existing.locator)
}

fn validate_repository_scope(
    repository_scope: Option<&RepositoryScope>,
) -> Result<(), TokenClaimsError> {
    if let Some(repository_scope) = repository_scope {
        let _validated = RepositoryScope::new(
            repository_scope.provider(),
            repository_scope.owner(),
            repository_scope.name(),
            repository_scope.revision(),
        )?;
    }

    Ok(())
}

const fn reconstruction_plan_error_detail(
    error: &FileRecordInvariantError,
) -> IndexRebuildIssueDetail {
    let detail = match error {
        FileRecordInvariantError::ChunkHash(_) => {
            IndexRebuildReconstructionPlanDetail::ChunkHashInvalid
        }
        FileRecordInvariantError::EmptyChunk => IndexRebuildReconstructionPlanDetail::EmptyChunk,
        FileRecordInvariantError::NonContiguousChunkOffsets => {
            IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets
        }
        FileRecordInvariantError::InvalidChunkRange => {
            IndexRebuildReconstructionPlanDetail::InvalidChunkRange
        }
        FileRecordInvariantError::InvalidPackedRange => {
            IndexRebuildReconstructionPlanDetail::InvalidPackedRange
        }
        FileRecordInvariantError::LengthOverflow => {
            IndexRebuildReconstructionPlanDetail::LengthOverflow
        }
        FileRecordInvariantError::TotalBytesMismatch => {
            IndexRebuildReconstructionPlanDetail::TotalBytesMismatch
        }
    };
    IndexRebuildIssueDetail::InvalidReconstructionPlan(detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::{LocalRecordStore, RecordMutation, RecordTraversal};
    use shardline_protocol::{HashParseError, RepositoryProvider, RepositoryScope};
    use std::time::Duration;

    fn valid_hex_hash() -> String {
        "a".repeat(64)
    }

    fn make_file_record(file_id: &str, content_hash: &str) -> FileRecord {
        FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: Vec::new(),
        }
    }

    fn make_file_record_with_scope(
        file_id: &str,
        content_hash: &str,
        scope: RepositoryScope,
    ) -> FileRecord {
        FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        }
    }

    fn make_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap()
    }

    // ---- collect_candidate tests ----

    fn empty_report() -> IndexRebuildReport {
        IndexRebuildReport {
            scanned_version_records: 0,
            scanned_retained_shards: 0,
            rebuilt_latest_records: 0,
            unchanged_latest_records: 0,
            removed_stale_latest_records: 0,
            scanned_reconstructions: 0,
            unchanged_reconstructions: 0,
            removed_stale_reconstructions: 0,
            rebuilt_dedupe_shard_mappings: 0,
            unchanged_dedupe_shard_mappings: 0,
            removed_stale_dedupe_shard_mappings: 0,
            preserved_latest_records_unreadable_version: Vec::new(),
            issues: Vec::new(),
        }
    }

    fn open_db(root: &std::path::Path) -> rusqlite::Connection {
        rusqlite::Connection::open(root.join("metadata.sqlite3"))
            .expect("failed to open metadata sqlite3 database")
    }

    /// Compute the length-prefixed record key for a scope-less version record.
    fn version_record_key(file_id: &str, content_hash: &str) -> String {
        // Each component is stored as "len:value"
        format!(
            "7:version8:6:global{len_fid}:{fid}{len_ch}:{ch}",
            len_fid = file_id.len(),
            fid = file_id,
            len_ch = content_hash.len(),
            ch = content_hash,
        )
    }

    /// Write a raw version record (bypassing the store) for error-path testing.
    /// Takes JSON bytes for the `record` column.
    fn write_raw_version_record(
        conn: &rusqlite::Connection,
        record_key: &str,
        file_id: &str,
        content_hash_val: &str,
        record_json: &[u8],
    ) {
        // scope_key for None (global)
        let scope_key = "6:global";
        conn.execute(
            "INSERT INTO shardline_file_records
                (record_key, record_kind, scope_key, file_id, content_hash, record, updated_at_unix_seconds)
             VALUES (?1, 'version', ?2, ?3, ?4, ?5, 1000)
             ON CONFLICT (record_key) DO UPDATE SET
                record = excluded.record",
            rusqlite::params![record_key, scope_key, file_id, content_hash_val, record_json],
        )
        .expect("failed to insert raw version record");
    }

    /// Pins a stored version record's `updated_at_unix_seconds` to an explicit
    /// value.
    ///
    /// The tie-breaker tests must not race the wall clock: two
    /// `write_version_record` calls stamp `unix_now_seconds_lossy()`, and under
    /// load they can straddle a second boundary — silently giving the second
    /// record a higher epoch so it wins even when the test wants the
    /// content-hash tiebreaker to decide. Writing through the store (faithful
    /// serialization + schema init) and then pinning the timestamps makes the
    /// epoch/hash comparison fully deterministic.
    fn pin_modified_since_epoch(
        root: &std::path::Path,
        record: &FileRecord,
        modified_since_epoch_secs: u64,
    ) {
        let conn = open_db(root);
        let key = version_record_key(&record.file_id, &record.content_hash);
        let updated = conn
            .execute(
                "UPDATE shardline_file_records
                    SET updated_at_unix_seconds = ?1
                  WHERE record_key = ?2",
                rusqlite::params![i64::try_from(modified_since_epoch_secs).unwrap(), &key],
            )
            .expect("failed to pin modified_since_epoch");
        assert_eq!(
            updated, 1,
            "expected exactly one row to be pinned for {key}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_valid_record_adds_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());
        let record = make_file_record("test.txt", &valid_hex_hash());

        // Write a valid version record
        store.write_version_record(&record).await.unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(candidates.len(), 1);
        // Verify the candidate fields match
        let candidate = candidates.values().next().unwrap();
        assert_eq!(candidate.record.file_id, "test.txt");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_invalid_file_id_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());
        // file_id with path traversal fails validate_identifier
        let record = make_file_record("../etc/passwd", &valid_hex_hash());
        store.write_version_record(&record).await.unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::InvalidVersionFileId
        );
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_invalid_content_hash_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());
        // Non-hex content hash fails validate_content_hash
        let record = make_file_record("test.txt", "not-a-valid-content-hash!!!");
        store.write_version_record(&record).await.unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::InvalidVersionContentHash
        );
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_invalid_repository_scope_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Write a valid record first to initialize the database
        let record = make_file_record("init.txt", &valid_hex_hash());
        store.write_version_record(&record).await.unwrap();

        // Now open the DB directly and insert a version record with an
        // invalid repository scope (empty owner).
        // The JSON deserialization will produce a RepositoryScope with
        // empty owner, which fails validate_repository_scope.
        let conn = open_db(&root);
        let invalid_json = br#"{
            "file_id": "scoped.txt",
            "content_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "total_bytes": 0,
            "chunk_size": 0,
            "repository_scope": {
                "provider": "GitHub",
                "owner": "",
                "name": "repo"
            },
            "chunks": []
        }"#;
        // record_key format: length-prefixed kind:scope_key:file_id
        // kind="version"(7), scope_key="6:global", file_id="scoped.txt"(10)
        write_raw_version_record(
            &conn,
            "7:version6:global10:scoped.txt",
            "scoped.txt",
            &valid_hex_hash(),
            invalid_json,
        );

        // Also delete the init record so only our test record is visited
        conn.execute(
            "DELETE FROM shardline_file_records WHERE file_id = 'init.txt'",
            [],
        )
        .unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::InvalidVersionRepositoryScope
        );
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_path_mismatch_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Write a valid record to initialize the database
        let record = make_file_record("init.txt", &valid_hex_hash());
        store.write_version_record(&record).await.unwrap();

        // Insert a version record whose record_key doesn't match what
        // version_record_locator would compute for the same record.
        // We use a different record_key to trigger the path mismatch.
        let conn = open_db(&root);
        let valid_json = serde_json::json!({
            "file_id": "mismatch.txt",
            "content_hash": valid_hex_hash(),
            "total_bytes": 0,
            "chunk_size": 0,
            "chunks": []
        });
        let json_bytes = serde_json::to_vec(&valid_json).unwrap();
        // Compute the correct key for this record, then pass a deliberately
        // different key to trigger path mismatch
        let correct_key = version_record_key("mismatch.txt", &valid_hex_hash());
        let wrong_key = correct_key.replace("mismatch.txt", "WRONG_____");
        write_raw_version_record(
            &conn,
            &wrong_key,
            "mismatch.txt",
            &valid_hex_hash(),
            &json_bytes,
        );

        // Delete the init record
        conn.execute(
            "DELETE FROM shardline_file_records WHERE file_id = 'init.txt'",
            [],
        )
        .unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::VersionPathMismatch
        );
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_invalid_reconstruction_plan_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());
        // EmptyChunk in the plan fails validate_reconstruction_plan
        use shardline_index::FileChunkRecord;
        let record = FileRecord {
            file_id: "test.txt".to_owned(),
            content_hash: valid_hex_hash(),
            total_bytes: 100,
            chunk_size: 100,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 0, // zero-length chunk => EmptyChunk error
                range_start: 0,
                range_end: 0,
                packed_start: 0,
                packed_end: 0,
            }],
        };
        store.write_version_record(&record).await.unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::InvalidVersionReconstructionPlan
        );
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_newer_version_replaces_existing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Create first record (older timestamp)
        let record1 = make_file_record("replace.txt", &valid_hex_hash());
        store.write_version_record(&record1).await.unwrap();

        // Create second record with SAME file_id but different content hash (newer)
        // Use a different file_id approach: write a separate version record
        // that rebuild_key maps to the same key, by having the same file_id
        let record2 = make_file_record("replace.txt", &"b".repeat(64));
        store.write_version_record(&record2).await.unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(candidates.len(), 1);
        // The candidate should have the newer content_hash ("b" > "a")
        let candidate = candidates.values().next().unwrap();
        // Both have the same file_id, same modified_since_epoch (same second),
        // so the tiebreaker is content_hash: "b".repeat(64) > valid_hex_hash()
        assert_eq!(
            candidate.record.content_hash,
            "b".repeat(64),
            "expected the lexicographically larger content hash to win"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_older_version_does_not_replace_newer() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Both records are pinned to the SAME `modified_since_epoch`, so the
        // content-hash tiebreaker decides ("b" > "a") regardless of wall-clock
        // drift between the two writes.
        let record_newer = make_file_record("older.txt", &"b".repeat(64));
        store.write_version_record(&record_newer).await.unwrap();
        let record_older = make_file_record("older.txt", &valid_hex_hash());
        store.write_version_record(&record_older).await.unwrap();
        pin_modified_since_epoch(&root, &record_newer, 1_700_000_000);
        pin_modified_since_epoch(&root, &record_older, 1_700_000_000);

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(candidates.len(), 1);
        // The candidate should have content_hash "b" because it was seen first
        // and "a" doesn't beat "b" in the tiebreaker
        let candidate = candidates.values().next().unwrap();
        assert_eq!(
            candidate.record.content_hash,
            "b".repeat(64),
            "expected the first-seen record with higher content hash to remain"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_older_record_does_not_replace_newer_existing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Two records with the same file_id and EXPLICIT epochs. Record "a" has
        // the higher epoch (100) and — because its lower content hash sorts
        // first — is processed first, so it wins; record "b" (epoch 1) is
        // processed second and hits the `Some(_) => {}` arm (not newer). The
        // explicit epochs make the comparison deterministic regardless of the
        // wall clock.
        let record_a = make_file_record("collide.txt", &"a".repeat(64));
        store.write_version_record(&record_a).await.unwrap();
        let record_b = make_file_record("collide.txt", &"b".repeat(64));
        store.write_version_record(&record_b).await.unwrap();
        pin_modified_since_epoch(&root, &record_a, 100);
        pin_modified_since_epoch(&root, &record_b, 1);

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(report.is_clean(), "report: {report:?}");
        assert_eq!(candidates.len(), 1);
        // The candidate should have content_hash "a" (the one with higher epoch).
        let candidate = candidates.values().next().unwrap();
        assert_eq!(candidate.record.content_hash, "a".repeat(64));

        // Verify that the newer content_hash record "b" was NOT selected
        // (because epoch 1 < epoch 100). This confirms the Some(_) => {} arm
        // fired for the second record (not newer, not replaced).
        assert_ne!(
            candidate.record.content_hash,
            "b".repeat(64),
            "content_hash 'b' should have been rejected due to lower epoch"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_bad_json_reports_issue() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        // Write a valid record to initialize the database
        let record = make_file_record("init.txt", &valid_hex_hash());
        store.write_version_record(&record).await.unwrap();

        // Insert non-JSON bytes directly
        let conn = open_db(&root);
        let bad_bytes = b"this is not valid json at all {{}}";
        write_raw_version_record(
            &conn,
            "7:version6:global8:bad.json",
            "bad.json",
            "a".repeat(64).as_str(),
            bad_bytes,
        );

        // Delete the init record
        conn.execute(
            "DELETE FROM shardline_file_records WHERE file_id = 'init.txt'",
            [],
        )
        .unwrap();

        let mut candidates = HashMap::new();
        let mut report = empty_report();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut report)
        })
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(
            report.issues[0].kind,
            IndexRebuildIssueKind::InvalidVersionRecordJson
        );
        assert!(candidates.is_empty());
    }

    // ---- rebuild_key tests ----

    #[test]
    fn rebuild_key_with_none_scope_has_no_repository_fields() {
        let record = make_file_record("file.txt", &valid_hex_hash());
        let key = rebuild_key(&record);

        assert_eq!(key.provider, None);
        assert_eq!(key.owner, None);
        assert_eq!(key.name, None);
        assert_eq!(key.revision, None);
        assert_eq!(key.file_id, "file.txt");
    }

    #[test]
    fn rebuild_key_with_scope_populates_all_fields() {
        let scope = make_scope();
        let record = make_file_record_with_scope("data.bin", &valid_hex_hash(), scope);
        let key = rebuild_key(&record);

        assert_eq!(key.provider, Some("github"));
        assert_eq!(key.owner, Some("team".to_owned()));
        assert_eq!(key.name, Some("repo".to_owned()));
        assert_eq!(key.revision, Some("main".to_owned()));
        assert_eq!(key.file_id, "data.bin");
    }

    #[test]
    fn rebuild_key_with_scope_without_revision() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "org", "project", None).unwrap();
        let record = make_file_record_with_scope("readme.md", &valid_hex_hash(), scope);
        let key = rebuild_key(&record);

        assert_eq!(key.provider, Some("gitlab"));
        assert_eq!(key.owner, Some("org".to_owned()));
        assert_eq!(key.name, Some("project".to_owned()));
        assert_eq!(key.revision, None);
        assert_eq!(key.file_id, "readme.md");
    }

    #[test]
    fn rebuild_key_uses_provider_directory_mapping() {
        for (provider, expected_dir) in [
            (RepositoryProvider::GitHub, "github"),
            (RepositoryProvider::Gitea, "gitea"),
            (RepositoryProvider::GitLab, "gitlab"),
            (RepositoryProvider::Codeberg, "codeberg"),
            (RepositoryProvider::Generic, "generic"),
        ] {
            let scope = RepositoryScope::new(provider, "owner", "name", None).unwrap();
            let record = make_file_record_with_scope("f", &valid_hex_hash(), scope);
            let key = rebuild_key(&record);
            assert_eq!(key.provider, Some(expected_dir));
        }
    }

    // ---- candidate_is_newer tests ----

    #[test]
    fn candidate_is_newer_when_modified_since_epoch_is_higher() {
        let older = VersionCandidate {
            record: make_file_record("f", &valid_hex_hash()),
            locator: "loc-a",
            modified_since_epoch: Duration::from_secs(100),
        };
        let newer = VersionCandidate {
            record: make_file_record("f", &valid_hex_hash()),
            locator: "loc-b",
            modified_since_epoch: Duration::from_secs(200),
        };

        assert!(candidate_is_newer(&newer, &older));
        assert!(!candidate_is_newer(&older, &newer));
    }

    #[test]
    fn candidate_is_newer_when_same_epoch_higher_content_hash() {
        let epoch = Duration::from_secs(100);
        let low_hash = VersionCandidate {
            record: make_file_record("f", &"0".repeat(64)),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let high_hash = VersionCandidate {
            record: make_file_record("f", &"f".repeat(64)),
            locator: "loc-b",
            modified_since_epoch: epoch,
        };

        assert!(candidate_is_newer(&high_hash, &low_hash));
        assert!(!candidate_is_newer(&low_hash, &high_hash));
    }

    #[test]
    fn candidate_is_newer_when_same_epoch_same_hash_higher_locator() {
        let epoch = Duration::from_secs(100);
        let hash = valid_hex_hash();
        let lower_locator = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let higher_locator = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-b",
            modified_since_epoch: epoch,
        };

        // "loc-b" > "loc-a"
        assert!(candidate_is_newer(&higher_locator, &lower_locator));
        assert!(!candidate_is_newer(&lower_locator, &higher_locator));
    }

    #[test]
    fn candidate_is_not_newer_when_equal() {
        let epoch = Duration::from_secs(100);
        let hash = valid_hex_hash();
        let a = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let b = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };

        assert!(!candidate_is_newer(&a, &b));
    }

    #[test]
    fn candidate_is_newer_epoch_beats_content_hash() {
        let lower_epoch = VersionCandidate {
            record: make_file_record("f", &"f".repeat(64)),
            locator: "loc-b",
            modified_since_epoch: Duration::from_secs(100),
        };
        let higher_epoch = VersionCandidate {
            record: make_file_record("f", &"0".repeat(64)),
            locator: "loc-a",
            modified_since_epoch: Duration::from_secs(200),
        };

        // Higher epoch wins even with lower content hash
        assert!(candidate_is_newer(&higher_epoch, &lower_epoch));
    }

    // ---- validate_repository_scope tests ----

    #[test]
    fn validate_repository_scope_none_returns_ok() {
        assert!(validate_repository_scope(None).is_ok());
    }

    #[test]
    fn validate_repository_scope_valid_scope_returns_ok() {
        let scope = make_scope();
        assert!(validate_repository_scope(Some(&scope)).is_ok());
    }

    #[test]
    fn validate_repository_scope_empty_owner_returns_err() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "", "repo", None);
        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryOwner));
    }

    #[test]
    fn validate_repository_scope_empty_name_returns_err() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "", None);
        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryName));
    }

    // ---- reconstruction_plan_error_detail tests ----

    #[test]
    fn reconstruction_plan_error_detail_chunk_hash() {
        let error = FileRecordInvariantError::ChunkHash(HashParseError::InvalidLength);
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::ChunkHashInvalid
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_empty_chunk() {
        let error = FileRecordInvariantError::EmptyChunk;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::EmptyChunk
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_non_contiguous_offsets() {
        let error = FileRecordInvariantError::NonContiguousChunkOffsets;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_chunk_range() {
        let error = FileRecordInvariantError::InvalidChunkRange;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::InvalidChunkRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_packed_range() {
        let error = FileRecordInvariantError::InvalidPackedRange;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::InvalidPackedRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_length_overflow() {
        let error = FileRecordInvariantError::LengthOverflow;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::LengthOverflow
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_total_bytes_mismatch() {
        let error = FileRecordInvariantError::TotalBytesMismatch;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::TotalBytesMismatch
            )
        );
    }
}
