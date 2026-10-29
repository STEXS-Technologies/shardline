use super::*;
use shardline_storage::{ObjectBody, ObjectIntegrity, S3ObjectStoreConfig};
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

#[test]
#[ignore = "requires owned Docker MinIO"]
fn remote_native_snapshot_uses_bounded_gets_and_still_detects_corruption() {
    let services = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("native FSCK request qualification requires Docker");
    let raw = services.s3_raw_config(None).unwrap();
    let body = b"native FSCK must authenticate the complete container";
    let length = u64::try_from(body.len()).unwrap();
    let member = compute_data_hash(body);
    let hash = xorb_hash(&[(member, length)]);
    let encoded = serialized_xorb_object_from_components(
        &hash,
        body.to_vec(),
        vec![(member, length)],
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let mut corrupt = encoded.clone();
    corrupt[10] ^= 1;
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::ReplaceSuccessfulGetBody(corrupt.into()),
    )
    .unwrap();
    let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_allow_http(true)
        .with_key_prefix(Some(
            &services.unique_s3_key_prefix("native-fsck-single-get"),
        ))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let store = ServerObjectStore::s3(config).unwrap();
    let hash = hash.hex();
    let key = xorb_object_key(&hash).unwrap();
    let integrity =
        ObjectIntegrity::new(chunk_hash(&encoded), u64::try_from(encoded.len()).unwrap());
    ObjectStore::put_if_absent(
        &store,
        &key,
        ObjectBody::from_bytes(encoded.into()),
        &integrity,
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let context = FsckObjectContext {
        object_root: root.path(),
        object_store: &store,
    };
    let term = FileChunkRecord {
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
    let member_key = chunk_object_key(&member.hex()).unwrap();
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Case {
        OptionalMissing,
        RequiredMissing,
        Valid,
        Corrupt,
        Oversized,
        SourceCorrupt,
    }
    use Case::*;
    for case in [
        OptionalMissing,
        RequiredMissing,
        Valid,
        Corrupt,
        Oversized,
        SourceCorrupt,
    ] {
        ObjectStore::delete_if_present(&store, &member_key).unwrap();
        let member_bytes = match case {
            Valid => Some(body.to_vec()),
            Corrupt => Some(vec![b'!'; body.len()]),
            Oversized => Some(vec![0; 2 * 1024 * 1024 + 1]),
            OptionalMissing | RequiredMissing | SourceCorrupt => None,
        };
        if let Some(bytes) = member_bytes {
            let integrity = ObjectIntegrity::new(chunk_hash(&bytes), bytes.len() as u64);
            ObjectStore::put_if_absent(
                &store,
                &member_key,
                ObjectBody::from_bytes(bytes.into()),
                &integrity,
            )
            .unwrap();
        }
        if case == SourceCorrupt {
            proxy.arm();
        }
        report.issues.clear();
        let gets = proxy.get_requests();
        let heads = proxy.head_requests();
        inspect_native_xet_term(
            &context,
            "native-record",
            &term,
            case != OptionalMissing,
            &mut reachability,
            &mut report,
            &mut None,
        )
        .unwrap();
        assert_eq!(
            proxy.get_requests() - gets,
            if case == SourceCorrupt { 1 } else { 2 },
            "{case:?}"
        );
        // Normal members and missing members need no preceding HEAD. Only an
        // oversized object uses metadata for its exact diagnostic length.
        assert_eq!(
            proxy.head_requests() - heads,
            usize::from(case == Oversized),
            "{case:?}"
        );
        match case {
            OptionalMissing | Valid => assert!(report.is_clean(), "{case:?}"),
            RequiredMissing => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::ReferencedByNativeXetXorb { .. }
            )),
            Corrupt => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::HashMismatch { .. }
            )),
            Oversized => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::LengthMismatch {
                    expected_length: 2097152,
                    observed_length: 2097153
                }
            )),
            SourceCorrupt => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::XorbHashMismatch { .. }
            )),
        }
    }
}
