//! Complete a delta composition with an existing source and upstream download.

use std::sync::Arc;

use reqwest::{Client, StatusCode};
use xet_core_structures::{
    merklehash::{MerkleHash, MerkleHashSubtree, compute_data_hash},
    metadata_shard::{
        chunk_verification::range_hash_from_chunks,
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
        },
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
    },
};

pub(super) async fn qualify(
    base: &str,
    read: &str,
    write: &str,
    stable_xorb: &str,
    composed: [u8; 32],
    proof: &serde_json::Value,
) {
    let gap: MerkleHashSubtree = serde_json::from_value(proof["hashRanges"][1].clone()).unwrap();
    let replacement =
        MerkleHashSubtree::from_chunks(true, &[(compute_data_hash(b"cccc"), 4)], false);
    let replacement: MerkleHashSubtree =
        serde_json::from_value(serde_json::to_value(replacement).unwrap()).unwrap();
    let root = MerkleHashSubtree::merge(&[replacement, gap])
        .unwrap()
        .final_hash()
        .unwrap();
    assert_eq!(
        blake3::keyed_hash(&[0; 32], root.as_bytes()).as_bytes(),
        &composed
    );
    let client = Client::new();
    let (changed_xorb, changed_hash) = super::single_chunk_xorb(b"cccc");
    let upload = client
        .post(format!("{base}/v1/xorbs/default/{changed_hash}"))
        .bearer_auth(write)
        .body(changed_xorb)
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);
    let parse = |hash: &str| {
        MerkleHash::from(
            shardline_index::parse_xet_hash_hex(hash)
                .unwrap()
                .as_bytes(),
        )
    };
    let identity = MerkleHash::from(&composed);
    let file = identity.hex();
    let stable_verification = parse(proof["gapVerification"][0].as_str().unwrap());
    let mut shard = MDBInMemoryShard::default();
    // No CAS block or uploaded body for the unchanged source: its existing
    // immutable object and the returned stable-segment proof are reused.
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(identity, 2, true, false),
        segments: vec![
            FileDataSequenceEntry::new(parse(&changed_hash), 4, 0, 1),
            FileDataSequenceEntry::new(parse(stable_xorb), 4, 0, 1),
        ],
        verification: vec![
            FileVerificationEntry::new(range_hash_from_chunks(&[compute_data_hash(b"cccc")])),
            FileVerificationEntry::new(stable_verification),
        ],
        metadata_ext: None,
    };
    let mut forged = info.clone();
    forged.metadata.file_hash = MerkleHash::from(&[1; 32]);
    let forged_file = forged.metadata.file_hash.hex();
    let mut forged_shard = MDBInMemoryShard::default();
    forged_shard.add_file_reconstruction_info(forged).unwrap();
    let mut forged_bytes = Vec::new();
    MDBShardInfo::serialize_from(&mut forged_bytes, &forged_shard, None).unwrap();
    let rejected = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(write)
        .body(forged_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        client
            .head(format!("{base}/v1/files/{forged_file}"))
            .bearer_auth(read)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    shard.add_file_reconstruction_info(info).unwrap();
    let mut serialized = Vec::new();
    MDBShardInfo::serialize_from(&mut serialized, &shard, None).unwrap();
    let registered = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(write)
        .body(serialized)
        .send()
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    let reconstruction = client
        .get(format!("{base}/v1/reconstructions/{file}"))
        .bearer_auth(read)
        .send()
        .await
        .unwrap();
    assert_eq!(reconstruction.status(), StatusCode::OK);
    let reconstruction: serde_json::Value = reconstruction.json().await.unwrap();
    assert_eq!(reconstruction["terms"][1]["hash"], stable_xorb);

    let endpoint = base.to_owned();
    let token = read.to_owned();
    let downloaded = tokio::task::spawn_blocking(move || {
        let runtime = xet_runtime::core::XetRuntime::new().unwrap();
        runtime
            .bridge_sync(async move {
                let temporary = tempfile::tempdir().unwrap();
                let mut config =
                    xet_data::processing::configurations::TranslatorConfig::test_server_config(
                        &endpoint,
                        temporary.path(),
                    )
                    .unwrap();
                config.session.auth =
                    xet_client::cas_client::auth::AuthConfig::maybe_new(Some(token), None, None);
                let download =
                    xet_data::processing::FileDownloadSession::new(Arc::new(config), None)
                        .await
                        .unwrap();
                let output = temporary.path().join("delta.out");
                download
                    .download_file(&xet_data::processing::XetFileInfo::new(file, 8), &output)
                    .await
                    .unwrap();
                std::fs::read(output).unwrap()
            })
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(downloaded, b"ccccbbbb");
}
