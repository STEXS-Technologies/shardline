use super::*;
use shardline_protocol::ShardlineHash;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        compression_scheme::CompressionScheme,
        xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

#[test]
fn snapshot_reuses_bytes_but_preserves_term_checks_and_record_freshness() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path_buf();
    let store = ServerObjectStore::local(root.clone()).unwrap();
    let data = b"verified container snapshot".to_vec();
    let length = u64::try_from(data.len()).unwrap();
    let member_hash = compute_data_hash(&data);
    let hash = xorb_hash(&[(member_hash, length)]);
    let encoded = serialized_xorb_object_from_components(
        &hash,
        data,
        vec![(member_hash, length)],
        CompressionScheme::None,
    )
    .unwrap();
    let hash = xet_hash_hex_string(ShardlineHash::from_bytes(hash.into()));
    let key = xorb_object_key(&hash).unwrap();
    let path = root.join(key.as_str());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, encoded.serialized_data).unwrap();
    let context = FsckObjectContext {
        object_root: &root,
        object_store: &store,
    };
    let mut term = FileChunkRecord {
        hash,
        offset: 0,
        length,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: length,
    };
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
    let mut reachability = FsckReachability::default();
    let mut snapshot = None;
    inspect_native_xet_term(
        &context,
        "record",
        &term,
        false,
        &mut reachability,
        &mut report,
        &mut snapshot,
    )
    .unwrap();
    assert!(report.is_clean());
    // If another term fetched or statted the container, this would fail.
    std::fs::remove_file(&path).unwrap();
    inspect_native_xet_term(
        &context,
        "record",
        &term,
        false,
        &mut reachability,
        &mut report,
        &mut snapshot,
    )
    .unwrap();
    assert!(report.is_clean());

    term.length = length.checked_add(1).unwrap();
    inspect_native_xet_term(
        &context,
        "record",
        &term,
        false,
        &mut reachability,
        &mut report,
        &mut snapshot,
    )
    .unwrap();
    assert!(matches!(
        report.issues.last().unwrap().detail,
        FsckIssueDetail::LengthMismatch { .. }
    ));
    term.length = length;
    term.range_end = 2;
    inspect_native_xet_term(
        &context,
        "record",
        &term,
        false,
        &mut reachability,
        &mut report,
        &mut snapshot,
    )
    .unwrap();
    assert!(matches!(
        report.issues.last().unwrap().detail,
        FsckIssueDetail::XorbRangeExceededChunkCount { .. }
    ));

    term.range_end = 1;
    inspect_native_xet_term(
        &context,
        "record",
        &term,
        true,
        &mut reachability,
        &mut report,
        &mut snapshot,
    )
    .unwrap();
    assert!(matches!(
        report.issues.last().unwrap().detail,
        FsckIssueDetail::ReferencedByNativeXetXorb { .. }
    ));

    // A new record must inspect the backend again and notice deletion.
    let mut next_record = None;
    inspect_native_xet_term(
        &context,
        "next-record",
        &term,
        false,
        &mut reachability,
        &mut report,
        &mut next_record,
    )
    .unwrap();
    assert!(matches!(
        report.issues.last().unwrap().detail,
        FsckIssueDetail::ReferencedByNativeXetRecord { .. }
    ));
}
