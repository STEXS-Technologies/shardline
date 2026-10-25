//! Recovery of candidates discarded by the released single-shard lookup.

use super::*;
use shardline_xet_core::{
    merklehash::{compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

const SHARED: &[u8] = b"shared native member";

fn fixture(tail: &[u8]) -> (Bytes, String, Bytes) {
    let chunks = vec![
        (compute_data_hash(SHARED), SHARED.len() as u64),
        (compute_data_hash(tail), tail.len() as u64),
    ];
    let hash = xorb_hash(&chunks);
    let payload = [SHARED, tail].concat();
    let object = serialized_xorb_object_from_components(
        &hash,
        payload.clone(),
        vec![
            (chunks[0].0, chunks[0].1),
            (chunks[1].0, payload.len() as u64),
        ],
        CompressionScheme::None,
    )
    .unwrap();
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(hash, 2, payload.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunks[0].0, chunks[0].1, 0)
                    .with_global_dedup_flag(true),
                XorbChunkSequenceEntry::new(chunks[1].0, chunks[1].1, chunks[0].1),
            ],
        })
        .unwrap();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&chunks), 1, false, false),
            segments: vec![FileDataSequenceEntry::new(hash, payload.len() as u64, 0, 2)],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    let mut serialized = Vec::new();
    MDBShardInfo::serialize_from(&mut serialized, &shard, None).unwrap();
    (object.serialized_data.into(), hash.hex(), serialized.into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_restores_both_scoped_sources_lost_by_single_legacy_mapping() {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let left =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", Some("main")).unwrap();
    let right =
        RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", Some("main")).unwrap();
    let denied =
        RepositoryScope::new(RepositoryProvider::GitHub, "eve", "model", Some("main")).unwrap();
    let mut expected = Vec::new();
    for (scope, tail) in [
        (&left, b"left tail".as_slice()),
        (&right, b"right tail".as_slice()),
    ] {
        let (object, hash, shard) = fixture(tail);
        backend.upload_xorb(&hash, object).await.unwrap();
        backend
            .upload_shard_stream(
                RequestBodyReader::from_bytes(shard),
                Some(scope),
                ShardMetadataLimits::default(),
            )
            .await
            .unwrap();
        expected.push(hash);
    }
    let member = compute_data_hash(SHARED).hex();
    let index = LocalIndexStore::open(root.path().to_path_buf());
    assert!(
        DedupeStore::dedupe_shard_mapping(&index, &parse_xet_hash_hex(&member).unwrap())
            .unwrap()
            .is_some()
    );
    // Simulate the predecessor's missing descriptor tables and remove decoded
    // copies so successful reconstruction must authenticate the retained Xorbs.
    Connection::open(root.path().join("metadata.sqlite3"))
        .unwrap()
        .execute("DELETE FROM shardline_xet_xorbs", [])
        .unwrap();
    for payload in [SHARED, b"left tail".as_slice(), b"right tail".as_slice()] {
        shardline_storage::AsyncObjectStore::delete_if_present(
            &backend.object_store(),
            &crate::chunk_store::chunk_object_key(&compute_data_hash(payload).hex()).unwrap(),
        )
        .await
        .unwrap();
    }
    assert!(
        backend
            .record_store()
            .xet_chunk_candidates_after(&member, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    let report = run_local_index_rebuild(root.path().to_path_buf())
        .await
        .unwrap();
    assert!(report.is_clean());
    drop(backend);
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let all = backend
        .record_store()
        .xet_chunk_candidates_after(&member, None, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    for (scope, hash) in [(&left, &expected[0]), (&right, &expected[1])] {
        let candidates = backend
            .record_store()
            .xet_chunk_candidates_after(&member, Some(scope), None)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(&candidates[0].hash, hash);
    }
    let backend = crate::ServerBackend::Local(backend);
    for scope in [&left, &right] {
        assert!(
            !backend
                .scoped_dedup_shard(&member, Some(scope), ShardMetadataLimits::default())
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert!(matches!(
        backend
            .scoped_dedup_shard(&member, Some(&denied), ShardMetadataLimits::default())
            .await,
        Err(ServerError::NotFound)
    ));
}
