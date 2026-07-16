#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::hub::HubRepoType;
use shardline_index::{LocalIndexStore, LocalRecordStore, parse_xet_hash_hex};

fuzz_target!(|data: &str| {
    let first_hash = parse_xet_hash_hex(data);
    let second_hash = parse_xet_hash_hex(data);
    assert_eq!(first_hash.is_ok(), second_hash.is_ok());

    let first_type = HubRepoType::parse_str(data);
    let second_type = HubRepoType::parse_str(data);
    assert_eq!(first_type, second_type);

    let first_from_api = HubRepoType::from_api_repo_type(data);
    let second_from_api = HubRepoType::from_api_repo_type(data);
    assert_eq!(first_from_api, second_from_api);

    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(_) => return,
    };
    let root = dir.path().to_path_buf();
    let index_store = match LocalIndexStore::new(root.clone()) {
        Ok(s) => s,
        Err(_) => return,
    };
    let record_store = match LocalRecordStore::new(root) {
        Ok(s) => s,
        Err(_) => return,
    };

    drop(index_store.insert_reconstruction(
        &shardline_index::FileId::new(shardline_protocol::ShardlineHash::from_bytes([1; 32])),
        &shardline_index::FileReconstruction::new(vec![]),
    ));
    drop(
        record_store.commit_file_version_metadata(&shardline_index::FileRecord {
            file_id: data.to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![],
        }),
    );
});
