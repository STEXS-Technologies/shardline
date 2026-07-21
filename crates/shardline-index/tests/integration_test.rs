#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::missing_const_for_fn,
    clippy::str_to_string,
    clippy::unwrap_in_result
)]

use shardline_index::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LocalIndexStore,
    ReconstructionStore, ReconstructionTerm, StoredObjectId,
};
use shardline_protocol::{ChunkRange, ShardlineHash};
use shardline_storage::ObjectKey;

fn make_file_id(byte: u8) -> FileId {
    FileId::new(ShardlineHash::from_bytes([byte; 32]))
}

fn make_object_id(byte: u8) -> StoredObjectId {
    StoredObjectId::new(ShardlineHash::from_bytes([byte; 32]))
}

fn make_reconstruction(term_count: u32) -> FileReconstruction {
    let object_id = make_object_id(1);
    let mut terms = Vec::new();
    for i in 0..term_count {
        let range = ChunkRange::new(i, i + 1).expect("valid range");
        terms.push(ReconstructionTerm::new(object_id, range, 64));
    }
    FileReconstruction::new(terms)
}

#[test]
fn local_index_store_inserts_and_lists_reconstructions() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let file_id = make_file_id(10);
    let reconstruction = make_reconstruction(2);

    store
        .insert_reconstruction(&file_id, &reconstruction)
        .expect("insert reconstruction");

    let ids = store
        .list_reconstruction_file_ids()
        .expect("list reconstructions");
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], file_id);

    let loaded = store.reconstruction(&file_id).expect("get reconstruction");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.terms().len(), 2);
}

#[test]
fn local_index_store_reconstruction_delete_is_idempotent() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let file_id = make_file_id(20);
    let reconstruction = make_reconstruction(1);

    store
        .insert_reconstruction(&file_id, &reconstruction)
        .expect("insert");

    let deleted = store.delete_reconstruction(&file_id).expect("delete first");
    assert!(deleted);

    let deleted = store
        .delete_reconstruction(&file_id)
        .expect("delete second");
    assert!(!deleted);

    let ids = store.list_reconstruction_file_ids().expect("list");
    assert!(ids.is_empty());
}

#[test]
fn local_index_store_inserts_and_queries_stored_objects() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let object_id = make_object_id(30);
    assert!(!store.contains_object(&object_id).expect("contains before"));

    store.insert_object(&object_id).expect("insert object");

    assert!(store.contains_object(&object_id).expect("contains after"));
}

#[test]
fn local_index_store_upserts_dedupe_shard_mappings() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let chunk_hash = ShardlineHash::from_bytes([40; 32]);
    let shard_key = ObjectKey::parse("shards/aa/chunk.shard").expect("valid key");
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);

    store
        .upsert_dedupe_shard_mapping(&mapping)
        .expect("upsert mapping");

    let loaded = store
        .dedupe_shard_mapping(&chunk_hash)
        .expect("get mapping");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.chunk_hash(), chunk_hash);
    assert_eq!(loaded.shard_object_key().as_str(), "shards/aa/chunk.shard");

    let all = store.list_dedupe_shard_mappings().expect("list mappings");
    assert_eq!(all.len(), 1);
}

#[test]
fn local_index_store_dedupe_mapping_delete_is_idempotent() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let chunk_hash = ShardlineHash::from_bytes([50; 32]);
    let shard_key = ObjectKey::parse("shards/bb/chunk.shard").expect("valid key");
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);

    store.upsert_dedupe_shard_mapping(&mapping).expect("upsert");

    let deleted = store
        .delete_dedupe_shard_mapping(&chunk_hash)
        .expect("delete first");
    assert!(deleted);

    let deleted = store
        .delete_dedupe_shard_mapping(&chunk_hash)
        .expect("delete second");
    assert!(!deleted);

    let all = store.list_dedupe_shard_mappings().expect("list");
    assert!(all.is_empty());
}

#[test]
fn local_index_store_multiple_reconstructions_are_independent() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let first_id = make_file_id(1);
    let second_id = make_file_id(2);
    let first_reconstruction = make_reconstruction(1);
    let second_reconstruction = make_reconstruction(3);

    store
        .insert_reconstruction(&first_id, &first_reconstruction)
        .expect("insert first");
    store
        .insert_reconstruction(&second_id, &second_reconstruction)
        .expect("insert second");

    let ids = store.list_reconstruction_file_ids().expect("list");
    assert_eq!(ids.len(), 2);

    let first = store.reconstruction(&first_id).expect("get first").unwrap();
    assert_eq!(first.terms().len(), 1);

    let second = store
        .reconstruction(&second_id)
        .expect("get second")
        .unwrap();
    assert_eq!(second.terms().len(), 3);
}

#[test]
fn local_index_store_reconstruction_missing_returns_none() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::new(root.path().to_path_buf()).expect("new store");

    let missing = make_file_id(99);
    let result = store.reconstruction(&missing).expect("get missing");
    assert!(result.is_none());
}

#[test]
fn local_index_store_open_is_non_mutating() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalIndexStore::open(root.path().to_path_buf());

    let ids = store.list_reconstruction_file_ids().expect("list on open");
    assert!(ids.is_empty());
}
