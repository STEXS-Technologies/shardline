//! Count real backend requests while the per-record snapshot turns over.
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
fn multi_member_snapshot_turnover_rechecks_sources_without_redundant_heads() {
    let services = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("multi-member FSCK qualification requires Docker");
    let raw = services.s3_raw_config(None).unwrap();
    let mut containers = Vec::new();
    for seed in [0_u8, 32] {
        let mut bytes = Vec::new();
        let mut leaves = Vec::new();
        let mut boundaries = Vec::new();
        for index in 0..16_u8 {
            let member = vec![seed + index; 64 * 1024];
            let hash = compute_data_hash(&member);
            leaves.push((hash, u64::try_from(member.len()).unwrap()));
            bytes.extend_from_slice(&member);
            boundaries.push((hash, u64::try_from(bytes.len()).unwrap()));
        }
        let hash = xorb_hash(&leaves);
        let encoded = serialized_xorb_object_from_components(
            &hash,
            bytes,
            boundaries,
            CompressionScheme::None,
        )
        .unwrap()
        .serialized_data;
        containers.push((hash.hex(), encoded));
    }
    let mut corrupt = containers.first().unwrap().1.clone();
    *corrupt.get_mut(10).unwrap() ^= 1;
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::ReplaceSuccessfulGetBody(corrupt.into()),
    )
    .unwrap();
    let store = ServerObjectStore::s3(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(Some(proxy.endpoint().to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some(&services.unique_s3_key_prefix("fsck-member-turnover")))
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            ),
    )
    .unwrap();
    for (hash, bytes) in &containers {
        ObjectStore::put_if_absent(
            &store,
            &xorb_object_key(hash).unwrap(),
            ObjectBody::from_slice(bytes),
            &ObjectIntegrity::new(chunk_hash(bytes), u64::try_from(bytes.len()).unwrap()),
        )
        .unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    let context = FsckObjectContext {
        object_root: root.path(),
        object_store: &store,
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
    for (index, expected_gets, damaged) in
        [(0, 17, false), (0, 16, false), (1, 17, false), (0, 1, true)]
    {
        if damaged {
            proxy.arm();
        }
        let term = FileChunkRecord {
            hash: containers.get(index).unwrap().0.clone(),
            offset: 0,
            length: 16 * 64 * 1024,
            range_start: 0,
            range_end: 16,
            packed_start: 0,
            packed_end: 16 * 64 * 1024,
        };
        let gets = proxy.get_requests();
        let heads = proxy.head_requests();
        inspect_native_xet_term(
            &context,
            "multi-member-record",
            &term,
            false,
            &mut reachability,
            &mut report,
            &mut snapshot,
        )
        .unwrap();
        assert_eq!(proxy.get_requests() - gets, expected_gets);
        assert_eq!(proxy.head_requests() - heads, 0);
        if damaged {
            assert!(snapshot.is_none());
            assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::XorbHashMismatch { .. }
            ));
        } else {
            assert!(report.is_clean());
        }
    }
}
