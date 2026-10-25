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
fn remote_native_snapshot_uses_one_get_and_still_detects_corruption() {
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
    for damaged in [false, true] {
        if damaged {
            proxy.arm();
        }
        let gets = proxy.get_requests();
        let heads = proxy.head_requests();
        inspect_native_xet_term(
            &context,
            "native-record",
            &term,
            false,
            &mut reachability,
            &mut report,
            &mut None,
        )
        .unwrap();
        assert_eq!(proxy.get_requests() - gets, 1);
        // Healthy inspection still checks the independent member object's
        // presence. There is no metadata request for the container itself.
        assert_eq!(proxy.head_requests() - heads, usize::from(!damaged));
        assert_eq!(report.is_clean(), !damaged);
        if damaged {
            assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::XorbHashMismatch { .. }
            ));
        }
    }
}
