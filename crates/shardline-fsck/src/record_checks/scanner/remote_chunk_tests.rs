use super::*;
use shardline_storage::{ObjectBody, ObjectIntegrity, S3ObjectStoreConfig};
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};

#[test]
#[ignore = "requires owned Docker MinIO"]
fn remote_ordinary_chunks_use_bounded_gets_without_losing_integrity_findings() {
    let services = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .expect("ordinary FSCK request qualification requires Docker");
    let raw = services.s3_raw_config(None).unwrap();
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::ReplaceSuccessfulGetBody(Vec::new().into()),
    )
    .unwrap();
    let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_allow_http(true)
        .with_key_prefix(Some(
            &services.unique_s3_key_prefix("ordinary-fsck-bounded-get"),
        ))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let store = ServerObjectStore::s3(config).unwrap();
    let root = tempfile::tempdir().unwrap();
    let body = b"ordinary chunks retain their hash and decompression checks";
    let length = body.len() as u64;
    let hash = xet_hash_hex_string(chunk_hash(body));
    let key = chunk_object_key(&hash).unwrap();
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
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Case {
        Missing,
        Valid,
        Corrupt,
        Oversized,
        Compressed,
        Bomb,
    }
    use Case::*;
    for case in [Valid, Missing, Corrupt, Oversized, Compressed, Bomb] {
        ObjectStore::delete_if_present(&store, &key).unwrap();
        let bytes = match case {
            Missing => None,
            Valid => Some(body.to_vec()),
            Corrupt => Some(vec![b'!'; body.len()]),
            Oversized => Some(vec![0; 2 * 1024 * 1024 + 1]),
            Compressed => Some(lz4_flex::compress_prepend_size(body)),
            Bomb => Some(u32::MAX.to_le_bytes().to_vec()),
        };
        let packed_length = bytes.as_ref().map_or(length, |bytes| bytes.len() as u64);
        if let Some(bytes) = bytes {
            let integrity = ObjectIntegrity::new(chunk_hash(&bytes), bytes.len() as u64);
            ObjectStore::put_if_absent(
                &store,
                &key,
                ObjectBody::from_bytes(bytes.into()),
                &integrity,
            )
            .unwrap();
        }
        let chunk = FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: if matches!(case, Compressed | Bomb) {
                packed_length
            } else {
                length
            },
        };
        let chunks = vec![chunk];
        let record = FileRecord {
            file_id: "ordinary-fsck".to_owned(),
            content_hash: content_hash(length, length, &chunks),
            total_bytes: length,
            chunk_size: length,
            storage_repr: if matches!(case, Compressed | Bomb) {
                StorageRepresentation::XorbCdcV1
            } else {
                StorageRepresentation::FixedChunkV1
            },
            repository_scope: None,
            chunks,
        };
        report.issues.clear();
        let gets = proxy.get_requests();
        let heads = proxy.head_requests();
        inspect_chunks(
            root.path(),
            "ordinary-record",
            &record,
            &store,
            &mut FsckReachability::default(),
            &mut report,
        )
        .unwrap();
        assert_eq!(proxy.get_requests() - gets, 1, "{case:?}");
        // Keep the legacy optional-container reachability probe. Compressed
        // single-chunk records also need their layout-disambiguation probe.
        let expected_heads = if matches!(case, Oversized | Compressed | Bomb) {
            2
        } else {
            1
        };
        assert_eq!(proxy.head_requests() - heads, expected_heads, "{case:?}");
        match case {
            Valid | Compressed => assert!(report.is_clean(), "{case:?}"),
            Missing => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::ReferencedByRecord { .. }
            )),
            Corrupt | Bomb => assert!(
                report
                    .issues
                    .iter()
                    .any(|issue| matches!(issue.detail, FsckIssueDetail::HashMismatch { .. }))
            ),
            Oversized => assert!(matches!(
                report.issues.last().unwrap().detail,
                FsckIssueDetail::LengthMismatch {
                    expected_length: 2097152,
                    observed_length: 2097153
                }
            )),
        }
    }
}
