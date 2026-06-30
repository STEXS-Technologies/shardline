use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, ObjectStore as _,
    PutOutcome,
};
use shardline_storage::LocalObjectStore;

fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

#[test]
fn local_object_store_put_and_get_roundtrip() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key = ObjectKey::parse("xorbs/default/aa/bb/data.xorb").expect("valid key");
    let body = b"hello world integration test payload";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let outcome = store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .expect("put");
    assert_eq!(outcome, PutOutcome::Inserted);

    let range = ByteRange::new(0, (body.len() as u64) - 1).expect("valid range");
    let read = store.read_range(&key, range).expect("read");
    assert_eq!(read, body);

    assert!(store.contains(&key).expect("contains"));

    let metadata = store.metadata(&key).expect("metadata").expect("some metadata");
    assert_eq!(metadata.length(), body.len() as u64);
}

#[test]
fn local_object_store_put_is_idempotent() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key = ObjectKey::parse("xorbs/default/cc/idem.xorb").expect("valid key");
    let body = b"identical content";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let first = store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .expect("first put");
    assert_eq!(first, PutOutcome::Inserted);

    let second = store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .expect("second put");
    assert_eq!(second, PutOutcome::AlreadyExists);
}

#[test]
fn local_object_store_rejects_integrity_mismatch() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key = ObjectKey::parse("xorbs/default/dd/mismatch.xorb").expect("valid key");
    let body = b"actual content";
    let wrong_hash = ShardlineHash::from_bytes([99; 32]);
    let wrong_integrity = ObjectIntegrity::new(wrong_hash, body.len() as u64);

    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &wrong_integrity);
    assert!(result.is_err());
}

#[test]
fn local_object_store_content_addressed_uniqueness() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key_a = ObjectKey::parse("xorbs/default/aa/object_a.xorb").expect("valid key");
    let key_b = ObjectKey::parse("xorbs/default/bb/object_b.xorb").expect("valid key");
    let body = b"shared content";

    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key_a, ObjectBody::from_slice(body), &integrity)
        .expect("put a");
    store
        .put_if_absent(&key_b, ObjectBody::from_slice(body), &integrity)
        .expect("put b");

    let range = ByteRange::new(0, (body.len() as u64) - 1).expect("range");
    let read_a = store.read_range(&key_a, range).expect("read a");
    let read_b = store.read_range(&key_b, range).expect("read b");
    assert_eq!(read_a, read_b);
    assert_eq!(read_a, body);
}

#[test]
fn local_object_store_prefix_listing() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key1 = ObjectKey::parse("xorbs/default/11/first.xorb").expect("key1");
    let key2 = ObjectKey::parse("xorbs/default/22/second.xorb").expect("key2");
    let key3 = ObjectKey::parse("other/prefix/third.xorb").expect("key3");

    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key1, ObjectBody::from_slice(body), &integrity)
        .expect("put1");
    store
        .put_if_absent(&key2, ObjectBody::from_slice(body), &integrity)
        .expect("put2");
    store
        .put_if_absent(&key3, ObjectBody::from_slice(body), &integrity)
        .expect("put3");

    let prefix = ObjectPrefix::parse("xorbs/default/").expect("prefix");
    let listed = store.list_prefix(&prefix).expect("list prefix");
    assert_eq!(listed.len(), 2);

    let keys: Vec<&ObjectKey> = listed.iter().map(|m| m.key()).collect();
    assert!(keys.contains(&&key1));
    assert!(keys.contains(&&key2));
    assert!(!keys.contains(&&key3));
}

#[test]
fn local_object_store_delete_and_not_found() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key = ObjectKey::parse("xorbs/default/ee/del.xorb").expect("key");
    let body = b"delete me";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .expect("put");

    let deleted = store.delete_if_present(&key).expect("delete");
    assert_eq!(deleted, DeleteOutcome::Deleted);

    let not_found = store.delete_if_present(&key).expect("delete again");
    assert_eq!(not_found, DeleteOutcome::NotFound);

    assert!(!store.contains(&key).expect("contains after delete"));
}

#[test]
fn local_object_store_read_range_out_of_bounds() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key = ObjectKey::parse("xorbs/default/ff/range.xorb").expect("key");
    let body = b"short";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .expect("put");

    let range = ByteRange::new(0, 100).expect("range");
    let result = store.read_range(&key, range);
    assert!(result.is_err());
}

#[test]
fn local_object_store_visit_prefix_collects_all_matching() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = LocalObjectStore::new(root.path().to_path_buf()).expect("new store");

    let key1 = ObjectKey::parse("shards/aa/one.shard").expect("key1");
    let key2 = ObjectKey::parse("shards/bb/two.shard").expect("key2");
    let body = b"payload";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key1, ObjectBody::from_slice(body), &integrity)
        .expect("put1");
    store
        .put_if_absent(&key2, ObjectBody::from_slice(body), &integrity)
        .expect("put2");

    let prefix = ObjectPrefix::parse("shards/").expect("prefix");
    let mut visited = Vec::new();
    store
        .visit_prefix(&prefix, |metadata| {
            visited.push(metadata.key().clone());
            Ok::<(), shardline_storage::LocalObjectStoreError>(())
        })
        .expect("visit");

    assert_eq!(visited.len(), 2);
}
