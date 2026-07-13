use std::{io::Cursor, path::Path};

use shardline_index::{
    FileChunkRecord, FileRecord, RecordTraversal, StoredRecord, parse_xet_hash_hex,
    xet_hash_hex_string,
};
use shardline_server_core::{
    OpsRecordStore, ServerObjectStore, checked_add, checked_increment, chunk_hash,
    chunk_object_key, content_hash, parse_stored_file_record_bytes, read_full_object,
    validate_content_hash, validate_identifier,
};
use shardline_storage::ObjectStore;
use shardline_xet_adapter::{
    XorbVisitError, try_for_each_serialized_xorb_chunk, validate_serialized_xorb, xorb_object_key,
};

use super::{
    FsckError, FsckIssueDetail, FsckIssueKind, FsckObjectContext, FsckReachability, FsckReport,
    PendingVersionRecordCheck, RecordKind, object_location_display, push_issue,
    push_reconstruction_plan_issue, record_path,
};

fn map_xorb_visit_error_fsck(error: XorbVisitError<FsckError>) -> FsckError {
    match error {
        XorbVisitError::Parse(error) => FsckError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}

pub(super) async fn scan_record_tree<RecordAdapter>(
    record_store: &RecordAdapter,
    record_kind: RecordKind,
    object_root: &Path,
    object_store: &ServerObjectStore,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<FsckError>,
{
    let object_context = FsckObjectContext {
        object_root,
        object_store,
    };
    match record_kind {
        RecordKind::Latest => {
            let mut pending_version_record_checks = Vec::new();
            RecordTraversal::visit_latest_records(record_store, |entry| {
                report.latest_records = checked_increment(report.latest_records)?;
                inspect_latest_record(
                    record_store,
                    entry,
                    &object_context,
                    reachability,
                    &mut pending_version_record_checks,
                    report,
                )
            })
            .await?;

            for check in pending_version_record_checks {
                if !RecordTraversal::record_locator_exists(record_store, &check.version_locator)
                    .await
                    .map_err(Into::into)?
                {
                    push_issue(
                        report,
                        FsckIssueKind::MissingVersionRecord,
                        record_store.locator_display(&check.latest_locator),
                        FsckIssueDetail::MissingVersionRecord {
                            version_locator: record_store.locator_display(&check.version_locator),
                        },
                    )?;
                    continue;
                }

                inspect_matching_version_record(record_store, &check, report).await?;
            }

            Ok(())
        }
        RecordKind::Version => {
            RecordTraversal::visit_version_records(record_store, |entry| {
                report.version_records = checked_increment(report.version_records)?;
                let _record = inspect_record_bytes(
                    record_store,
                    &entry,
                    RecordKind::Version,
                    &object_context,
                    reachability,
                    report,
                )?;
                Ok(())
            })
            .await
        }
    }
}

fn inspect_latest_record<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: StoredRecord<RecordAdapter::Locator>,
    object_context: &FsckObjectContext<'_>,
    reachability: &mut FsckReachability,
    pending_version_record_checks: &mut Vec<PendingVersionRecordCheck<RecordAdapter::Locator>>,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    RecordAdapter: OpsRecordStore,
    RecordAdapter::Error: Into<FsckError>,
{
    let Some(record) = inspect_record_bytes(
        record_store,
        &entry,
        RecordKind::Latest,
        object_context,
        reachability,
        report,
    )?
    else {
        return Ok(());
    };
    let StoredRecord {
        locator: path,
        bytes: _bytes,
        modified_since_epoch: _modified_since_epoch,
    } = entry;

    pending_version_record_checks.push(PendingVersionRecordCheck {
        latest_locator: path,
        version_locator: record_store.version_record_locator(&record),
        latest_record: record,
    });

    Ok(())
}

fn inspect_record_bytes<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: &StoredRecord<RecordAdapter::Locator>,
    record_kind: RecordKind,
    object_context: &FsckObjectContext<'_>,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<Option<FileRecord>, FsckError>
where
    RecordAdapter: OpsRecordStore,
{
    let path = &entry.locator;
    let bytes = &entry.bytes;
    let record_location = record_store.locator_display(path);
    let record = match parse_stored_file_record_bytes(bytes) {
        Ok(record) => record,
        Err(shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            ..
        }) => {
            push_issue(
                report,
                FsckIssueKind::OversizedRecordMetadata,
                record_location,
                FsckIssueDetail::OversizedRecordMetadata,
            )?;
            return Ok(None);
        }
        Err(shardline_server_core::ParseStoredFileRecordError::Json(_)) => {
            push_issue(
                report,
                FsckIssueKind::InvalidRecordJson,
                record_location,
                FsckIssueDetail::RecordJsonInvalid,
            )?;
            return Ok(None);
        }
    };

    if validate_identifier(&record.file_id).is_err() {
        push_issue(
            report,
            FsckIssueKind::InvalidFileId,
            record_store.locator_display(path),
            FsckIssueDetail::InvalidFileId {
                file_id: record.file_id.clone(),
            },
        )?;
    }

    if validate_content_hash(&record.content_hash).is_err() {
        push_issue(
            report,
            FsckIssueKind::InvalidContentHash,
            record_store.locator_display(path),
            FsckIssueDetail::InvalidContentHash {
                content_hash: record.content_hash.clone(),
            },
        )?;
    }

    let expected_path = record_path(record_store, record_kind, &record);
    if expected_path != *path {
        push_issue(
            report,
            FsckIssueKind::RecordPathMismatch,
            record_store.locator_display(path),
            FsckIssueDetail::RecordPathMismatch {
                expected_locator: record_store.locator_display(&expected_path),
            },
        )?;
    }

    let expected_file_id = record_store.locator_file_id(path, record_kind.ops());
    if expected_file_id.as_deref() != Some(record.file_id.as_str()) {
        push_issue(
            report,
            FsckIssueKind::RecordPathMismatch,
            record_store.locator_display(path),
            FsckIssueDetail::RecordFileIdPathMismatch,
        )?;
    }

    if record_kind == RecordKind::Version {
        let expected_hash = record_store.locator_content_hash(path, record_kind.ops());
        if expected_hash.as_deref() != Some(record.content_hash.as_str()) {
            push_issue(
                report,
                FsckIssueKind::RecordPathMismatch,
                record_store.locator_display(path),
                FsckIssueDetail::RecordContentHashPathMismatch,
            )?;
        }
    }

    if let Err(error) = record.validate_reconstruction_plan() {
        push_reconstruction_plan_issue(report, record_store.locator_display(path), &error)?;
        return Ok(Some(record));
    }

    inspect_chunks(
        object_context.object_root,
        &record_location,
        &record,
        object_context.object_store,
        reachability,
        report,
    )?;

    Ok(Some(record))
}

fn inspect_chunks(
    object_root: &Path,
    record_location: &str,
    record: &FileRecord,
    object_store: &ServerObjectStore,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError> {
    for chunk in &record.chunks {
        report.inspected_chunk_references = checked_increment(report.inspected_chunk_references)?;

        if record.chunk_size == 0 {
            inspect_native_xet_term(
                object_root,
                object_store,
                record_location,
                chunk,
                reachability,
                report,
            )?;
            continue;
        }

        let object_key = match chunk_object_key(&chunk.hash) {
            Ok(object_key) => object_key,
            Err(_) => {
                push_issue(
                    report,
                    FsckIssueKind::InvalidContentHash,
                    record_location.to_owned(),
                    FsckIssueDetail::InvalidChunkHash {
                        chunk_hash: chunk.hash.clone(),
                    },
                )?;
                continue;
            }
        };
        reachability
            .referenced_object_keys
            .insert(object_key.as_str().to_owned());
        reachability
            .live_dedupe_chunk_hashes
            .insert(chunk.hash.clone());
        let optional_xorb_key = xorb_object_key(&chunk.hash)?;
        if object_store.metadata(&optional_xorb_key)?.is_some() {
            reachability
                .referenced_object_keys
                .insert(optional_xorb_key.as_str().to_owned());
        }

        let chunk_location = object_location_display(object_root, object_store, &object_key);
        let metadata = match object_store.metadata(&object_key)? {
            Some(metadata) => metadata,
            None => {
                push_issue(
                    report,
                    FsckIssueKind::MissingChunk,
                    chunk_location.clone(),
                    FsckIssueDetail::ReferencedByRecord {
                        record_location: record_location.to_owned(),
                    },
                )?;
                continue;
            }
        };

        let chunk_bytes = read_full_object(object_store, &object_key, metadata.length())?;
        let actual_hash = xet_hash_hex_string(chunk_hash(&chunk_bytes));
        if actual_hash != chunk.hash {
            push_issue(
                report,
                FsckIssueKind::ChunkHashMismatch,
                chunk_location.clone(),
                FsckIssueDetail::HashMismatch {
                    expected_hash: chunk.hash.clone(),
                    observed_hash: actual_hash,
                },
            )?;
        }

        let actual_length = metadata.length();
        if actual_length != chunk.length {
            push_issue(
                report,
                FsckIssueKind::ChunkLengthMismatch,
                chunk_location,
                FsckIssueDetail::LengthMismatch {
                    expected_length: chunk.length,
                    observed_length: actual_length,
                },
            )?;
        }
    }

    let computed_content_hash = content_hash(record.total_bytes, record.chunk_size, &record.chunks);
    if computed_content_hash != record.content_hash {
        push_issue(
            report,
            FsckIssueKind::RecordHashMismatch,
            record_location.to_owned(),
            FsckIssueDetail::HashMismatch {
                expected_hash: record.content_hash.clone(),
                observed_hash: computed_content_hash,
            },
        )?;
    }

    Ok(())
}

fn inspect_native_xet_term(
    object_root: &Path,
    object_store: &ServerObjectStore,
    record_location: &str,
    chunk: &FileChunkRecord,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError> {
    let object_key = match xorb_object_key(&chunk.hash) {
        Ok(object_key) => object_key,
        Err(_) => {
            push_issue(
                report,
                FsckIssueKind::InvalidContentHash,
                record_location.to_owned(),
                FsckIssueDetail::InvalidXorbHash {
                    xorb_hash: chunk.hash.clone(),
                },
            )?;
            return Ok(());
        }
    };
    reachability
        .referenced_object_keys
        .insert(object_key.as_str().to_owned());

    let xorb_location = object_location_display(object_root, object_store, &object_key);
    let metadata = match object_store.metadata(&object_key)? {
        Some(metadata) => metadata,
        None => {
            push_issue(
                report,
                FsckIssueKind::MissingChunk,
                xorb_location,
                FsckIssueDetail::ReferencedByNativeXetRecord {
                    record_location: record_location.to_owned(),
                },
            )?;
            return Ok(());
        }
    };

    let xorb_bytes = read_full_object(object_store, &object_key, metadata.length())?;
    let expected_hash = parse_xet_hash_hex(&chunk.hash)?;
    let mut reader = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    let range_start = usize::try_from(chunk.range_start)?;
    let range_end = usize::try_from(chunk.range_end)?;
    if range_end > validated.chunks().len() {
        push_issue(
            report,
            FsckIssueKind::ChunkLengthMismatch,
            xorb_location,
            FsckIssueDetail::XorbRangeExceededChunkCount {
                range_start: chunk.range_start,
                range_end: chunk.range_end,
                chunk_count: validated.chunks().len(),
            },
        )?;
        return Ok(());
    }

    let mut actual_length = 0_u64;
    let mut chunk_index = 0_usize;
    try_for_each_serialized_xorb_chunk(&mut reader, &validated, |decoded_chunk| {
        if chunk_index < range_start || chunk_index >= range_end {
            chunk_index = chunk_index.checked_add(1).ok_or(FsckError::Overflow)?;
            return Ok::<(), FsckError>(());
        }

        let unpacked_length = u64::try_from(decoded_chunk.data().len())?;
        actual_length = checked_add(actual_length, unpacked_length)?;

        let chunk_hash_hex = xet_hash_hex_string(decoded_chunk.descriptor().hash());
        let chunk_object_key = chunk_object_key(&chunk_hash_hex)?;
        reachability
            .referenced_object_keys
            .insert(chunk_object_key.as_str().to_owned());
        let chunk_location = object_location_display(object_root, object_store, &chunk_object_key);
        let chunk_metadata = match object_store.metadata(&chunk_object_key)? {
            Some(chunk_metadata) => chunk_metadata,
            None => {
                push_issue(
                    report,
                    FsckIssueKind::MissingChunk,
                    chunk_location,
                    FsckIssueDetail::ReferencedByNativeXetXorb {
                        xorb_location: xorb_location.clone(),
                    },
                )?;
                chunk_index = chunk_index.checked_add(1).ok_or(FsckError::Overflow)?;
                return Ok(());
            }
        };

        let chunk_bytes =
            read_full_object(object_store, &chunk_object_key, chunk_metadata.length())?;
        let actual_chunk_hash = xet_hash_hex_string(chunk_hash(&chunk_bytes));
        if actual_chunk_hash != chunk_hash_hex {
            push_issue(
                report,
                FsckIssueKind::ChunkHashMismatch,
                chunk_location.clone(),
                FsckIssueDetail::HashMismatch {
                    expected_hash: chunk_hash_hex,
                    observed_hash: actual_chunk_hash,
                },
            )?;
        }
        if chunk_metadata.length() != unpacked_length {
            push_issue(
                report,
                FsckIssueKind::ChunkLengthMismatch,
                chunk_location,
                FsckIssueDetail::LengthMismatch {
                    expected_length: unpacked_length,
                    observed_length: chunk_metadata.length(),
                },
            )?;
        }

        chunk_index = chunk_index.checked_add(1).ok_or(FsckError::Overflow)?;
        Ok(())
    })
    .map_err(map_xorb_visit_error_fsck)?;
    if actual_length != chunk.length {
        push_issue(
            report,
            FsckIssueKind::ChunkLengthMismatch,
            object_location_display(object_root, object_store, &object_key),
            FsckIssueDetail::LengthMismatch {
                expected_length: chunk.length,
                observed_length: actual_length,
            },
        )?;
    }

    Ok(())
}

async fn inspect_matching_version_record<RecordAdapter>(
    record_store: &RecordAdapter,
    check: &PendingVersionRecordCheck<RecordAdapter::Locator>,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<FsckError>,
{
    let version_bytes = RecordTraversal::read_record_bytes(record_store, &check.version_locator)
        .await
        .map_err(Into::into)?;
    let version_record = match parse_stored_file_record_bytes(&version_bytes) {
        Ok(record) => record,
        Err(shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            ..
        }) => {
            push_issue(
                report,
                FsckIssueKind::OversizedRecordMetadata,
                record_store.locator_display(&check.version_locator),
                FsckIssueDetail::OversizedRecordMetadata,
            )?;
            return Ok(());
        }
        Err(_error) => {
            return Ok(());
        }
    };
    if version_record != check.latest_record {
        push_issue(
            report,
            FsckIssueKind::MismatchedVersionRecord,
            record_store.locator_display(&check.latest_locator),
            FsckIssueDetail::MismatchedVersionRecord {
                version_locator: record_store.locator_display(&check.version_locator),
            },
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── map_xorb_visit_error_fsck ─────────────────────────────────────────

    #[test]
    fn map_xorb_visit_error_parse_wraps_parse_error() {
        let err =
            XorbVisitError::<FsckError>::Parse(shardline_xet_adapter::XorbParseError::HashMismatch);
        let result = map_xorb_visit_error_fsck(err);
        assert!(matches!(result, FsckError::Overflow));
    }

    #[test]
    fn map_xorb_visit_error_visitor_passthrough() {
        let err = XorbVisitError::<FsckError>::Visitor(FsckError::Overflow);
        let result = map_xorb_visit_error_fsck(err);
        assert!(matches!(result, FsckError::Overflow));
    }

    #[test]
    fn map_xorb_visit_error_visitor_passthrough_roundtrip() {
        let err = XorbVisitError::<FsckError>::Visitor(FsckError::Io(std::io::Error::other(
            "test",
        )));
        let result = map_xorb_visit_error_fsck(err);
        assert!(matches!(result, FsckError::Io(_)));
    }

    // ── scan_record_tree ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_empty_store_latest_returns_ok() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Latest,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree(Latest) failed: {result:?}");
        assert_eq!(report.latest_records, 0);
        // On an empty store, there are also no pending version-record checks,
        // so the report stays clean.
        assert!(report.is_clean());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_empty_store_version_returns_ok() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Version,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree(Version) failed: {result:?}");
        assert_eq!(report.version_records, 0);
        assert!(report.is_clean());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_latest_reports_orphan_missing_version() {
        use shardline_index::RecordMutation;

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Create a valid record with no chunks and the matching content hash.
        let chunks = Vec::new();
        let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
        let record = shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash,
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks,
        };

        // Write only the latest record (no matching version record).
        record_store.write_latest_record(&record).await.unwrap();

        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Latest,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree failed: {result:?}");
        assert_eq!(report.latest_records, 1);

        // Expect a MissingVersionRecord issue because we only wrote the latest.
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.kind == FsckIssueKind::MissingVersionRecord),
            "expected MissingVersionRecord issue, got: {report:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_latest_with_matching_version_is_clean() {
        use shardline_index::RecordMutation;

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Create a valid record with no chunks and the matching content hash.
        let chunks = Vec::new();
        let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
        let record = shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash,
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks,
        };

        // Write both the latest and version records so the version check passes.
        record_store.write_latest_record(&record).await.unwrap();
        record_store.write_version_record(&record).await.unwrap();

        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Latest,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree failed: {result:?}");
        assert_eq!(report.latest_records, 1);

        // With both records present there should be no issues.
        assert!(report.is_clean(), "expected clean report, got: {report:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_version_with_valid_record_is_clean() {
        use shardline_index::RecordMutation;

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Create a valid record with no chunks and the matching content hash.
        let chunks = Vec::new();
        let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
        let record = shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash,
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks,
        };

        record_store.write_version_record(&record).await.unwrap();

        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Version,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree failed: {result:?}");
        assert_eq!(report.version_records, 1);
        assert!(report.is_clean(), "expected clean report, got: {report:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_record_tree_mismatched_version_record_reported() {
        use shardline_index::RecordMutation;

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // To trigger MismatchedVersionRecord the version record must exist at the
        // locator that the latest record's check expects, but its content must
        // differ.  Since the version locator includes content_hash, give both
        // records the *same* content_hash but differ other fields.
        let shared_hash = "ab".repeat(32); // 64-char valid hex hash

        let latest_record = shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash: shared_hash.clone(),
            total_bytes: 100,
            chunk_size: 4096,
            repository_scope: None,
            chunks: vec![shardline_index::FileChunkRecord {
                hash: "cd".repeat(32),
                offset: 0,
                length: 100,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 100,
            }],
        };

        // Version record at the same locator (same file_id + content_hash) but
        // with different content (different total_bytes/chunks so the full
        // struct comparison fails).
        let version_record = shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash: shared_hash,
            total_bytes: 200,
            chunk_size: 4096,
            repository_scope: None,
            chunks: Vec::new(),
        };

        record_store.write_latest_record(&latest_record).await.unwrap();
        record_store
            .write_version_record(&version_record)
            .await
            .unwrap();

        let mut reachability = FsckReachability::default();
        let mut report = FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        };

        let result = scan_record_tree(
            &record_store,
            RecordKind::Latest,
            &object_root,
            &object_store,
            &mut reachability,
            &mut report,
        )
        .await;
        assert!(result.is_ok(), "scan_record_tree failed: {result:?}");
        assert_eq!(report.latest_records, 1);

        // The version record exists at the expected locator but content differs.
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.kind == FsckIssueKind::MismatchedVersionRecord),
            "expected MismatchedVersionRecord issue, got: {report:?}"
        );
    }
}
