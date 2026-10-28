//! Qualify fragmented registration against an actual S3-compatible backend.
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_server_core::{DEFAULT_SHARD_METADATA_LIMITS, ServerObjectStore};
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo,
        },
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker MinIO for registration qualification"]
async fn fragmented_registration_preserves_sha256_on_real_minio() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_minio_tmpfs()
        .start()
        .unwrap()
        .expect("MinIO qualification requires Docker");
    let raw = services
        .s3_raw_config(Some("registration-coalescing"))
        .unwrap();
    let objects = ServerObjectStore::s3(
        shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            )
            .with_allow_http(true),
    )
    .unwrap();
    // Distinct, incompressible-enough members; both layouts describe the same
    // logical file, including a backwards jump after the first complete run.
    let mut data = vec![0_u8; 8 * 1024 * 1024];
    let mut fixture = blake3::Hasher::new();
    fixture.update(b"fragmented-registration-minio-fixture");
    fixture.finalize_xof().fill(&mut data);
    let leaves: Vec<_> = data
        .chunks(64 * 1024)
        .map(|chunk| (compute_data_hash(chunk), chunk.len() as u64))
        .collect();
    let hash = xorb_hash(&leaves);
    let boundaries = leaves
        .iter()
        .enumerate()
        .map(|(index, (hash, _))| (*hash, ((index + 1) * 64 * 1024) as u64))
        .collect();
    let xorb = serialized_xorb_object_from_components(
        &hash,
        data.clone(),
        boundaries,
        CompressionScheme::None,
    )
    .unwrap();
    let prepared =
        shardline_xet_adapter::prepare_xorb_memory(&hash.hex(), &xorb.serialized_data).unwrap();
    shardline_xet_adapter::store_prepared_xorb_memory(&objects, prepared, ())
        .await
        .unwrap();
    let mut digest = Sha256::new();
    digest.update(&data);
    digest.update(&data);
    let expected = format!("{:x}", digest.finalize());
    let all_leaves: Vec<_> = leaves.iter().chain(&leaves).copied().collect();
    let full = FileDataSequenceEntry::new(hash, data.len() as u64, 0, leaves.len() as u64);
    for fragmented in [false, true] {
        let mut segments = vec![full.clone()];
        if fragmented {
            segments.extend(leaves.iter().enumerate().map(|(index, (_, length))| {
                FileDataSequenceEntry::new(hash, *length, index as u64, (index + 1) as u64)
            }));
        } else {
            segments.push(full.clone());
        }
        let file = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&all_leaves),
                segments.len() as u64,
                false,
                true,
            ),
            segments,
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&expected).unwrap(),
            )),
        };
        let mut shard = MDBInMemoryShard::default();
        shard.add_file_reconstruction_info(file).unwrap();
        let mut bytes = Vec::new();
        MDBShardInfo::serialize_from(&mut bytes, &shard, None).unwrap();
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "qualification",
            // The second registration must exercise the same verified-prefix
            // cache as the first while retaining real remote member reads.
            "equivalent-fragmented",
            None,
        )
        .unwrap();
        let objects = objects.clone();
        let expected = expected.clone();
        tokio::task::spawn_blocking(move || {
            let parsed = shardline_xet_adapter::parse_uploaded_shard(
                &objects,
                &bytes,
                Some(&scope),
                DEFAULT_SHARD_METADATA_LIMITS,
            )
            .unwrap();
            assert_eq!(parsed.file_identities.first().unwrap().sha256, expected);
            assert_eq!(
                parsed.records.first().unwrap().total_bytes,
                16 * 1024 * 1024
            );
        })
        .await
        .unwrap();
    }
}
