use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    time::Duration,
};

use shardline_index::{FileRecord, FileRecordInvariantError, RecordTraversal, StoredRecord};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{OpsRecordStore, validate_content_hash, validate_identifier};

use super::{
    IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReconstructionPlanDetail,
    IndexRebuildReport, push_issue,
};
use shardline_server_core::parse_stored_file_record_bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionCandidate<Locator> {
    pub(super) record: FileRecord,
    pub(super) locator: Locator,
    pub(super) modified_since_epoch: Duration,
}

// Checked RepositoryScope deserialization rejects invalid components before a
// FileRecord can be constructed. Preserve the rebuild diagnostic without ever
// exposing an unchecked scope or accepting a malformed record as a candidate.
fn has_invalid_repository_components(bytes: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct ScopeFields {
        provider: shardline_protocol::RepositoryProvider,
        owner: String,
        name: String,
        revision: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct ScopeEnvelope {
        repository_scope: Option<ScopeFields>,
    }
    let Ok(ScopeEnvelope {
        repository_scope: Some(scope),
    }) = serde_json::from_slice(bytes)
    else {
        return false;
    };
    RepositoryScope::new(
        scope.provider,
        &scope.owner,
        &scope.name,
        scope.revision.as_deref(),
    )
    .is_err()
}

pub(super) fn collect_candidate<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: StoredRecord<RecordAdapter::Locator>,
    candidates: &mut HashMap<RecordAdapter::Locator, VersionCandidate<RecordAdapter::Locator>>,
    native_xorbs: &mut HashSet<String>,
    report: &mut IndexRebuildReport,
) where
    RecordAdapter: OpsRecordStore,
    RecordAdapter::Locator: Hash,
{
    let StoredRecord {
        locator: path,
        bytes,
        modified_since_epoch,
    } = entry;
    let location = || record_store.locator_display(&path);
    let record = match parse_stored_file_record_bytes(&bytes) {
        Ok(record) => record,
        Err(shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            ..
        }) => {
            return reject_version(
                report,
                IndexRebuildIssueKind::OversizedVersionRecordMetadata,
                location(),
                IndexRebuildIssueDetail::OversizedVersionRecordMetadata,
            );
        }
        Err(shardline_server_core::ParseStoredFileRecordError::Json(_)) => {
            let (kind, detail) = if has_invalid_repository_components(&bytes) {
                (
                    IndexRebuildIssueKind::InvalidVersionRepositoryScope,
                    IndexRebuildIssueDetail::InvalidRepositoryScope,
                )
            } else {
                (
                    IndexRebuildIssueKind::InvalidVersionRecordJson,
                    IndexRebuildIssueDetail::RecordJsonInvalid,
                )
            };
            return reject_version(report, kind, location(), detail);
        }
    };

    if record_store.supports_native_locations()
        && record.storage_layout()
            == shardline_index::FileRecordStorageLayout::ReferencedObjectTerms
    {
        native_xorbs.extend(record.chunks.iter().map(|term| term.hash.clone()));
    }

    if validate_identifier(&record.file_id).is_err() {
        return reject_version(
            report,
            IndexRebuildIssueKind::InvalidVersionFileId,
            location(),
            IndexRebuildIssueDetail::InvalidFileId {
                file_id: record.file_id,
            },
        );
    }

    if validate_content_hash(&record.content_hash).is_err() {
        return reject_version(
            report,
            IndexRebuildIssueKind::InvalidVersionContentHash,
            location(),
            IndexRebuildIssueDetail::InvalidContentHash {
                content_hash: record.content_hash,
            },
        );
    }

    let expected_path = RecordTraversal::version_record_locator(record_store, &record);
    if expected_path != path {
        return reject_version(
            report,
            IndexRebuildIssueKind::VersionPathMismatch,
            location(),
            IndexRebuildIssueDetail::VersionPathMismatch {
                expected_locator: record_store.locator_display(&expected_path),
            },
        );
    }

    if let Err(error) = record.validate_reconstruction_plan() {
        return reject_version(
            report,
            IndexRebuildIssueKind::InvalidVersionReconstructionPlan,
            location(),
            reconstruction_plan_error_detail(&error),
        );
    }

    let key = RecordTraversal::latest_record_locator(record_store, &record);
    let candidate = VersionCandidate {
        record,
        locator: path,
        modified_since_epoch,
    };
    match candidates.entry(key) {
        Entry::Occupied(mut slot) if candidate_is_newer(&candidate, slot.get()) => {
            slot.insert(candidate);
        }
        Entry::Vacant(slot) => {
            slot.insert(candidate);
        }
        Entry::Occupied(_) => {}
    }
}

// Every rejected version dirties the scan and records why its existing head
// must be preserved. Keep these two report updates together.
fn reject_version(
    report: &mut IndexRebuildReport,
    kind: IndexRebuildIssueKind,
    location: String,
    detail: IndexRebuildIssueDetail,
) {
    push_issue(report, kind, location.clone(), detail);
    report
        .preserved_latest_records_unreadable_version
        .push(location);
}

fn candidate_is_newer<Locator>(
    candidate: &VersionCandidate<Locator>,
    existing: &VersionCandidate<Locator>,
) -> bool
where
    Locator: Ord,
{
    (
        candidate.modified_since_epoch,
        &candidate.record.content_hash,
        &candidate.locator,
    ) > (
        existing.modified_since_epoch,
        &existing.record.content_hash,
        &existing.locator,
    )
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

    fn assert_rejected_version(report: &IndexRebuildReport, kind: IndexRebuildIssueKind) {
        assert!(!report.is_clean());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.issues[0].kind, kind);
        assert_eq!(
            report.preserved_latest_records_unreadable_version,
            [report.issues[0].location.clone()]
        );
    }

    fn valid_hex_hash() -> String {
        "a".repeat(64)
    }

    fn make_file_record(file_id: &str, content_hash: &str) -> FileRecord {
        FileRecord {
            deduplication_domain: None,
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
            repository_scope: Some(scope),
            ..make_file_record(file_id, content_hash)
        }
    }

    // ---- collect_candidate tests ----

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
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
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
    async fn invalid_candidate_records_report_issues_and_remain_unselected() {
        use shardline_index::FileChunkRecord;

        for (record, kind) in [
            (
                make_file_record("../etc/passwd", &valid_hex_hash()),
                IndexRebuildIssueKind::InvalidVersionFileId,
            ),
            (
                make_file_record("test.txt", "not-a-valid-content-hash!!!"),
                IndexRebuildIssueKind::InvalidVersionContentHash,
            ),
            (
                FileRecord {
                    deduplication_domain: None,
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
                },
                IndexRebuildIssueKind::InvalidVersionReconstructionPlan,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalRecordStore::open(dir.path().to_path_buf());
            store.write_version_record(&record).await.unwrap();

            let mut candidates = HashMap::new();
            let mut xorbs = HashSet::new();
            let mut report = IndexRebuildReport::default();
            RecordTraversal::visit_version_records(&store, |entry| {
                collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
                Ok::<(), crate::RebuildError>(())
            })
            .await
            .unwrap();

            assert_rejected_version(&report, kind);
            assert!(candidates.is_empty());
        }
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
        // Checked JSON deserialization rejects the scope; rebuild still
        // reports the specific repository-component diagnostic.
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
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
        })
        .await
        .unwrap();

        assert_rejected_version(
            &report,
            IndexRebuildIssueKind::InvalidVersionRepositoryScope,
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
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
        })
        .await
        .unwrap();

        assert_rejected_version(&report, IndexRebuildIssueKind::VersionPathMismatch);
        assert!(candidates.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_candidate_newer_version_replaces_existing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let store = LocalRecordStore::open(root.clone());

        for (content, source) in [("a", "c"), ("b", "d")] {
            let mut record = make_file_record("replace.txt", &content.repeat(64));
            record.total_bytes = 1;
            record.chunks = vec![shardline_index::FileChunkRecord {
                hash: source.repeat(64),
                offset: 0,
                length: 1,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 1,
            }];
            store.write_version_record(&record).await.unwrap();
            pin_modified_since_epoch(&root, &record, 100);
        }

        let mut candidates = HashMap::new();
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
        })
        .await
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(candidates.len(), 1);
        assert_eq!(xorbs, HashSet::from(["c".repeat(64), "d".repeat(64)]));
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

    #[tokio::test]
    async fn collect_candidate_selects_latest_epoch_then_content_hash() {
        for (epoch_a, epoch_b, expected_hash) in [(100, 100, "b"), (100, 1, "a"), (1, 100, "b")] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let store = LocalRecordStore::open(root.to_path_buf());
            // Insert in reverse hash order; pin timestamps so wall-clock drift
            // cannot change which comparison decides the winner.
            for (hash, epoch) in [("b", epoch_b), ("a", epoch_a)] {
                let record = make_file_record("versions.txt", &hash.repeat(64));
                store.write_version_record(&record).await.unwrap();
                pin_modified_since_epoch(root, &record, epoch);
            }

            let mut candidates = HashMap::new();
            let mut xorbs = HashSet::new();
            let mut report = IndexRebuildReport::default();
            RecordTraversal::visit_version_records(&store, |entry| {
                collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
                Ok::<(), crate::RebuildError>(())
            })
            .await
            .unwrap();

            assert!(report.is_clean(), "{report:?}");
            assert_eq!(candidates.len(), 1);
            assert_eq!(
                candidates.values().next().unwrap().record.content_hash,
                expected_hash.repeat(64),
                "epochs: a={epoch_a}, b={epoch_b}"
            );
        }
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
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();

        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
        })
        .await
        .unwrap();

        assert_rejected_version(&report, IndexRebuildIssueKind::InvalidVersionRecordJson);
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn candidate_grouping_preserves_repository_identity_revision_and_file_id() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalRecordStore::open(root.path().to_path_buf());
        let hash = valid_hex_hash();
        store
            .write_version_record(&make_file_record("data.bin", &hash))
            .await
            .unwrap();
        for provider in [
            RepositoryProvider::GitHub,
            RepositoryProvider::Gitea,
            RepositoryProvider::GitLab,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ] {
            for revision in [None, Some("main")] {
                let scope = RepositoryScope::new(provider, "team", "repo", revision).unwrap();
                store
                    .write_version_record(&make_file_record_with_scope("data.bin", &hash, scope))
                    .await
                    .unwrap();
            }
        }
        for (owner, repo, revision, file_id) in [
            ("other", "repo", Some("main"), "data.bin"),
            ("team", "other", Some("main"), "data.bin"),
            ("team", "repo", Some("other"), "data.bin"),
            ("team", "repo", Some("main"), "other.bin"),
        ] {
            let scope =
                RepositoryScope::new(RepositoryProvider::GitHub, owner, repo, revision).unwrap();
            store
                .write_version_record(&make_file_record_with_scope(file_id, &hash, scope))
                .await
                .unwrap();
        }
        let mut candidates = HashMap::new();
        let mut xorbs = HashSet::new();
        let mut report = IndexRebuildReport::default();
        RecordTraversal::visit_version_records(&store, |entry| {
            collect_candidate(&store, entry, &mut candidates, &mut xorbs, &mut report);
            Ok::<(), crate::RebuildError>(())
        })
        .await
        .unwrap();
        assert!(report.is_clean(), "{report:?}");
        assert_eq!(candidates.len(), 15);
    }

    // ---- candidate_is_newer tests ----

    #[test]
    fn candidate_ordering_uses_epoch_then_hash_then_locator() {
        fn candidate(
            (seconds, hash, locator): (u64, &str, &'static str),
        ) -> VersionCandidate<&'static str> {
            VersionCandidate {
                record: make_file_record("f", &hash.repeat(64)),
                locator,
                modified_since_epoch: Duration::from_secs(seconds),
            }
        }

        for (lower, higher, strictly_ordered) in [
            // Timestamp decides when hashes match.
            ((100, "a", "loc-a"), (200, "a", "loc-b"), true),
            // Hash decides when timestamps match.
            ((100, "0", "loc-a"), (100, "f", "loc-b"), true),
            // Locator breaks a timestamp/hash tie.
            ((100, "a", "loc-a"), (100, "a", "loc-b"), true),
            // Equal candidates never replace each other.
            ((100, "a", "loc-a"), (100, "a", "loc-a"), false),
            // Timestamp takes priority over both hash and locator.
            ((100, "f", "loc-b"), (200, "0", "loc-a"), true),
            // Hash takes priority over locator.
            ((100, "0", "loc-b"), (100, "f", "loc-a"), true),
        ] {
            let lower = candidate(lower);
            let higher = candidate(higher);
            assert_eq!(
                candidate_is_newer(&higher, &lower),
                strictly_ordered,
                "higher: {higher:?}, lower: {lower:?}"
            );
            assert!(
                !candidate_is_newer(&lower, &higher),
                "lower: {lower:?}, higher: {higher:?}"
            );
        }
    }

    // ---- reconstruction_plan_error_detail tests ----

    #[test]
    fn reconstruction_plan_errors_preserve_details() {
        for (error, expected) in [
            (
                FileRecordInvariantError::ChunkHash(HashParseError::InvalidLength),
                IndexRebuildReconstructionPlanDetail::ChunkHashInvalid,
            ),
            (
                FileRecordInvariantError::EmptyChunk,
                IndexRebuildReconstructionPlanDetail::EmptyChunk,
            ),
            (
                FileRecordInvariantError::NonContiguousChunkOffsets,
                IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets,
            ),
            (
                FileRecordInvariantError::InvalidChunkRange,
                IndexRebuildReconstructionPlanDetail::InvalidChunkRange,
            ),
            (
                FileRecordInvariantError::InvalidPackedRange,
                IndexRebuildReconstructionPlanDetail::InvalidPackedRange,
            ),
            (
                FileRecordInvariantError::LengthOverflow,
                IndexRebuildReconstructionPlanDetail::LengthOverflow,
            ),
            (
                FileRecordInvariantError::TotalBytesMismatch,
                IndexRebuildReconstructionPlanDetail::TotalBytesMismatch,
            ),
        ] {
            assert_eq!(
                reconstruction_plan_error_detail(&error),
                IndexRebuildIssueDetail::InvalidReconstructionPlan(expected)
            );
        }
    }
}
