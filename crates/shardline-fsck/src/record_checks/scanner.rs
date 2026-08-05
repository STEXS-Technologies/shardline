use std::{io::Cursor, path::Path};

// ── Re-exported to parent module so tests (via super::*) can see them ──
pub(super) use shardline_index::{
    FileChunkRecord, FileRecord, RecordTraversal, StorageRepresentation, StoredRecord,
    parse_xet_hash_hex, xet_hash_hex_string,
};
pub(super) use shardline_server_core::{
    OpsRecordStore, ServerObjectStore, checked_add, checked_increment, chunk_hash,
    chunk_object_key, content_hash, parse_stored_file_record_bytes, read_full_object,
    validate_content_hash, validate_identifier,
};
pub(super) use shardline_storage::ObjectStore;
pub(super) use shardline_xet_adapter::{
    try_for_each_serialized_xorb_chunk, validate_serialized_xorb, xorb_object_key,
};

pub(super) use crate::{
    FsckError, FsckIssueDetail, FsckIssueKind, FsckObjectContext, FsckReachability, FsckReport,
    PendingVersionRecordCheck, RecordKind, object_location_display, push_issue,
    push_reconstruction_plan_issue, record_path,
};

pub(super) use super::mapping::map_xorb_visit_error_fsck;

pub(crate) async fn scan_record_tree<RecordAdapter>(
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

pub(crate) fn inspect_latest_record<RecordAdapter>(
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

pub(crate) fn inspect_record_bytes<RecordAdapter>(
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

/// Returns whether a single XorbCdcV1 chunk record is xorb-backed.
///
/// The ingestor repoints a single-chunk record's `hash` to the packed xorb hash
/// and sets `packed_end` to the chunk's serialized length inside the xorb.
/// `packed_end > 0` alone is not a discriminator: legacy pre-packing records also
/// carry `packed_end = compressed_length > 0` with `hash` as the raw chunk hash.
/// The stored-object probe below separates repointed records (a xorb object exists
/// under the record hash) from legacy records (no such object).
fn single_chunk_is_xorb_backed(
    object_store: &ServerObjectStore,
    chunk: &FileChunkRecord,
) -> Result<bool, FsckError> {
    if chunk.packed_end == 0 {
        return Ok(false); // legacy record predating xorb packing
    }
    let Ok(object_key) = xorb_object_key(&chunk.hash) else {
        return Ok(false);
    };
    Ok(matches!(object_store.metadata(&object_key)?, Some(metadata) if metadata.length() != 0))
}

pub(crate) fn inspect_chunks(
    object_root: &Path,
    record_location: &str,
    record: &FileRecord,
    object_store: &ServerObjectStore,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError> {
    // Determine whether this record's chunks are xorb-backed terms. Native Xet
    // records (chunk_size == 0) always reference a xorb container directly.
    // XorbCdcV1 records reference a xorb container when the ingestor repointed
    // their hashes: all chunks of a multi-chunk file share one xorb hash, and a
    // single-chunk record is xorb-backed only when a stored xorb object exists
    // under its hash (the probe separates repointed records from legacy
    // pre-packing records whose hash is the raw chunk hash).
    let single_chunk_xorb_backed = if record.chunks.len() == 1 {
        single_chunk_is_xorb_backed(
            object_store,
            record.chunks.first().ok_or(FsckError::Overflow)?,
        )?
    } else {
        false
    };
    let all_chunks_xorb_backed = record.chunk_size == 0
        || (record.storage_repr == StorageRepresentation::XorbCdcV1
            && (record.chunks.len() > 1 || single_chunk_xorb_backed));

    for chunk in &record.chunks {
        report.inspected_chunk_references = checked_increment(report.inspected_chunk_references)?;

        if all_chunks_xorb_backed {
            inspect_native_xet_term(
                object_root,
                object_store,
                record_location,
                chunk,
                record.chunk_size == 0,
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
        // XorbCdcV1 chunks are stored LZ4-compressed with a 4-byte
        // little-endian uncompressed-size prefix (mirror the download path in
        // download_stream.rs). The record's packed_end is the compressed
        // storage length: when it differs from the raw chunk length, the
        // stored object must be decompressed before hash and length are
        // verified. FixedChunkV1 records keep packed_end == chunk.length.
        let (actual_hash, actual_length) = if chunk.packed_end != chunk.length {
            // Guard against corrupt size prefixes before allocating (mirror
            // the download path's MAX_DECOMPRESSED_CHUNK bound).
            let decompressed_size = chunk_bytes
                .first_chunk::<4>()
                .map(|header| u32::from_le_bytes(*header) as u64)
                .unwrap_or(u64::MAX);
            if decompressed_size > 2 * 1024 * 1024 {
                (
                    xet_hash_hex_string(chunk_hash(&chunk_bytes)),
                    metadata.length(),
                )
            } else {
                match lz4_flex::decompress_size_prepended(&chunk_bytes) {
                    Ok(decompressed) => (
                        xet_hash_hex_string(chunk_hash(&decompressed)),
                        u64::try_from(decompressed.len())?,
                    ),
                    // A blob that fails to decompress is corrupt: report the
                    // stored bytes as the observed hash and length.
                    Err(_error) => (
                        xet_hash_hex_string(chunk_hash(&chunk_bytes)),
                        metadata.length(),
                    ),
                }
            }
        } else {
            (
                xet_hash_hex_string(chunk_hash(&chunk_bytes)),
                metadata.length(),
            )
        };
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

pub(crate) fn inspect_native_xet_term(
    object_root: &Path,
    object_store: &ServerObjectStore,
    record_location: &str,
    chunk: &FileChunkRecord,
    require_member_chunks: bool,
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
    // A stored container whose bytes fail to parse or validate is corrupt:
    // report it as a chunk hash mismatch instead of failing the whole fsck
    // run. The reader is an in-memory cursor over already-read bytes, so any
    // parse error here (including Io from out-of-bounds seeks) is corruption.
    let validated = match validate_serialized_xorb(&mut reader, expected_hash) {
        Ok(validated) => validated,
        Err(_error) => {
            push_issue(
                report,
                FsckIssueKind::ChunkHashMismatch,
                xorb_location,
                FsckIssueDetail::XorbHashMismatch {
                    expected_hash: chunk.hash.clone(),
                },
            )?;
            return Ok(());
        }
    };
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
                // Native Xet records always store the individual chunk
                // objects alongside the container.  XorbCdcV1 records from
                // the ingestor store only the container, so a missing
                // member object is expected there and must not be reported.
                if require_member_chunks {
                    push_issue(
                        report,
                        FsckIssueKind::MissingChunk,
                        chunk_location,
                        FsckIssueDetail::ReferencedByNativeXetXorb {
                            xorb_location: xorb_location.clone(),
                        },
                    )?;
                }
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

pub(crate) async fn inspect_matching_version_record<RecordAdapter>(
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
