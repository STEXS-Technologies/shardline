//! Integration tests for `shardline-server-core`.
//!
//! These tests exercise the public API — [`ServerObjectStore`] with local
//! mode, validation helpers (`validate_identifier`, `validate_content_hash`),
//! and checked arithmetic (`checked_add`, `checked_increment`) — through
//! temporary filesystem roots and in-memory values.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use shardline_server_core::{
    ServerObjectStore, ShardMetadataLimits, checked_add, checked_increment, validate_content_hash,
    validate_identifier,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

// ============================================================================
// ServerObjectStore — local mode
// ============================================================================

#[test]
fn local_store_create_and_put() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/hello.txt").expect("valid key");
    let data = b"hello world";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    let outcome = store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put should succeed");
    assert_eq!(outcome, shardline_storage::PutOutcome::Inserted);
}

#[test]
fn local_store_put_twice_returns_already_present() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/dup.txt").expect("valid key");
    let data = b"duplicate";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );

    let first = store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("first put");
    assert_eq!(first, shardline_storage::PutOutcome::Inserted);

    let second = store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("second put");
    assert_eq!(second, shardline_storage::PutOutcome::AlreadyExists);
}

#[test]
fn local_store_contains_put_object() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/exists.bin").expect("valid key");
    let data = b"i am here";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put");

    assert!(store.contains(&key).expect("contains check"));
}

#[test]
fn local_store_not_contains_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/ghost.bin").expect("valid key");
    assert!(!store.contains(&key).expect("contains check"));
}

#[test]
fn local_store_read_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/data.bin").expect("valid key");
    let data = b"0123456789";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put");

    let range = shardline_protocol::ByteRange::new(1, 4).expect("valid range");
    let contents = store.read_range(&key, range).expect("read_range");
    assert_eq!(contents, b"1234");
}

#[test]
fn local_store_delete_if_present() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/delete-me.bin").expect("valid key");
    let data = b"delete me";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put");

    let outcome = store.delete_if_present(&key).expect("delete");
    assert_eq!(outcome, shardline_storage::DeleteOutcome::Deleted);

    assert!(!store.contains(&key).expect("should be gone"));
}

#[test]
fn local_store_delete_absent_returns_not_found() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/never-existed.bin").expect("valid key");
    let outcome = store.delete_if_present(&key).expect("delete");
    assert_eq!(outcome, shardline_storage::DeleteOutcome::NotFound);
}

#[test]
fn local_store_read_full_object() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/full-read.bin").expect("valid key");
    let data = b"full object read test";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put");

    let length = u64::try_from(data.len()).unwrap_or(0);
    let contents =
        shardline_server_core::read_full_object(&store, &key, length).expect("read_full_object");
    assert_eq!(contents, data);
}

#[test]
fn local_store_put_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    let key = ObjectKey::parse("test/overwrite.bin").expect("valid key");
    let data = b"original";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_overwrite(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put_overwrite");

    let new_data = b"overwritten";
    let new_integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(new_data),
        u64::try_from(new_data.len()).unwrap_or(0),
    );
    store
        .put_overwrite(&key, ObjectBody::Borrowed(new_data), &new_integrity)
        .expect("put_overwrite again");

    let length = u64::try_from(new_data.len()).unwrap_or(0);
    let contents = shardline_server_core::read_full_object(&store, &key, length)
        .expect("read after overwrite");
    assert_eq!(contents, new_data);
}

#[test]
fn local_store_list_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");

    for i in 0..3 {
        let key = ObjectKey::parse(&format!("list/prefix/item-{i}.bin")).expect("valid key");
        let data = vec![i as u8; 16];
        let integrity = ObjectIntegrity::new(
            shardline_server_core::chunk_hash(&data),
            u64::try_from(data.len()).unwrap_or(0),
        );
        store
            .put_if_absent(&key, ObjectBody::from_vec(data), &integrity)
            .expect("put");
    }

    let prefix = shardline_storage::ObjectPrefix::parse("list/prefix/").expect("valid prefix");
    let entries = store.list_prefix(&prefix).expect("list_prefix");
    assert_eq!(entries.len(), 3);
}

#[test]
fn local_store_backend_name() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("objects")).expect("create local store");
    assert_eq!(store.backend_name(), "local");
}

#[test]
fn blackhole_backend_name() {
    let store = ServerObjectStore::blackhole();
    assert_eq!(store.backend_name(), "blackhole");
}

#[test]
fn blackhole_put_always_inserted() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("blackhole/test").expect("valid key");
    let data = b"gone";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    let outcome = store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("blackhole put");
    assert_eq!(outcome, shardline_storage::PutOutcome::Inserted);
}

#[test]
fn blackhole_not_contains_anything() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("blackhole/ghost").expect("valid key");
    assert!(!store.contains(&key).expect("contains blackhole"));
}

#[test]
fn blackhole_read_returns_not_found() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("blackhole/absent").expect("valid key");
    let range = shardline_protocol::ByteRange::new(0, 0).expect("valid range");
    let result = store.read_range(&key, range);
    assert!(
        result.is_err(),
        "blackhole read_range should error: {:?}",
        result
    );
}

// ============================================================================
// validate_identifier
// ============================================================================

#[test]
fn validate_identifier_valid_simple() {
    assert!(validate_identifier("readme.md").is_ok());
    assert!(validate_identifier("file.txt").is_ok());
    assert!(validate_identifier("a").is_ok());
}

#[test]
fn validate_identifier_rejects_empty() {
    assert!(validate_identifier("").is_err());
}

#[test]
fn validate_identifier_rejects_traversal() {
    assert!(validate_identifier("../etc/passwd").is_err());
    assert!(validate_identifier("a/../../b").is_err());
}

#[test]
fn validate_identifier_rejects_absolute() {
    assert!(validate_identifier("/etc/hosts").is_err());
}

#[test]
fn validate_identifier_rejects_control_characters() {
    assert!(validate_identifier("file\nname").is_err());
    assert!(validate_identifier("file\tname").is_err());
}

// ============================================================================
// validate_content_hash
// ============================================================================

#[test]
fn validate_content_hash_valid_hex() {
    assert!(validate_content_hash(&"a".repeat(64)).is_ok());
    assert!(validate_content_hash(&"0".repeat(64)).is_ok());
    assert!(validate_content_hash(&"f".repeat(64)).is_ok());
    assert!(
        validate_content_hash("9abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678")
            .is_ok()
    );
}

#[test]
fn validate_content_hash_rejects_short() {
    assert!(validate_content_hash(&"a".repeat(63)).is_err());
    assert!(validate_content_hash("").is_err());
    assert!(validate_content_hash("abc").is_err());
}

#[test]
fn validate_content_hash_rejects_long() {
    assert!(validate_content_hash(&"a".repeat(65)).is_err());
}

#[test]
fn validate_content_hash_rejects_uppercase() {
    assert!(validate_content_hash(&"A".repeat(64)).is_err());
}

#[test]
fn validate_content_hash_rejects_non_hex() {
    assert!(validate_content_hash(&"z".repeat(64)).is_err());
    assert!(validate_content_hash(&"g".repeat(64)).is_err());
}

// ============================================================================
// checked_add / checked_increment
// ============================================================================

#[test]
fn checked_add_basic() {
    let result = checked_add(10u64, 20u64).expect("10 + 20 should fit");
    assert_eq!(result, 30);
}

#[test]
fn checked_add_zero() {
    let result = checked_add(0u64, 0u64).expect("0 + 0 should fit");
    assert_eq!(result, 0);
}

#[test]
fn checked_add_max_plus_zero() {
    let result = checked_add(u64::MAX, 0u64).expect("MAX + 0 should fit");
    assert_eq!(result, u64::MAX);
}

#[test]
fn checked_add_overflow_error() {
    let result = checked_add(u64::MAX, 1u64);
    assert!(result.is_err(), "MAX + 1 should overflow");
}

#[test]
fn checked_increment_basic() {
    let result = checked_increment(41u64).expect("41 + 1 should fit");
    assert_eq!(result, 42);
}

#[test]
fn checked_increment_zero() {
    let result = checked_increment(0u64).expect("0 + 1 should fit");
    assert_eq!(result, 1);
}

#[test]
fn checked_increment_max_overflows() {
    let result = checked_increment(u64::MAX);
    assert!(result.is_err(), "MAX + 1 should overflow");
}

#[test]
fn checked_increment_chain() {
    let mut value = 0u64;
    for _ in 0..10 {
        value = checked_increment(value).expect("increment should fit");
    }
    assert_eq!(value, 10);
}

// ============================================================================
// ShardMetadataLimits
// ============================================================================

#[test]
fn shard_metadata_limits_default() {
    let limits = ShardMetadataLimits::default();
    assert!(limits.max_xorbs().get() > 0);
    assert!(limits.max_xorb_chunks().get() > 0);
    assert!(limits.max_files().get() > 0);
    assert!(limits.max_reconstruction_terms().get() > 0);
}

// ============================================================================
// Edge cases: create local store with nested directory
// ============================================================================

#[test]
fn local_store_deeply_nested_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("deep")).expect("create local store");

    let key = ObjectKey::parse("a/b/c/d/e/f/g/data.bin").expect("valid key");
    let data = b"nested";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    let outcome = store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put nested");
    assert_eq!(outcome, shardline_storage::PutOutcome::Inserted);
    assert!(store.contains(&key).expect("contains nested"));
}

#[test]
fn local_store_large_key_name() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("large-key")).expect("create local store");

    let long_name = "x".repeat(200);
    let key = ObjectKey::parse(&long_name).expect("valid long key");
    let data = b"large key test";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(data),
        u64::try_from(data.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(data), &integrity)
        .expect("put long key");
}

#[test]
fn local_store_root_returns_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root_path = dir.path().join("myobjects");
    let store = ServerObjectStore::local(root_path.clone()).expect("create local store");

    let returned_root = store.local_root().expect("local_root should be Some");
    assert_eq!(returned_root, root_path);
}

#[test]
fn blackhole_local_root_is_none() {
    let store = ServerObjectStore::blackhole();
    assert!(store.local_root().is_none());
}

#[test]
fn blackhole_local_path_for_key_is_none() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("any/key").expect("valid key");
    assert!(store.local_path_for_key(&key).is_none());
}

#[test]
fn local_store_local_path_for_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("p")).expect("create local store");

    let key = ObjectKey::parse("some/object.bin").expect("valid key");
    let path = store
        .local_path_for_key(&key)
        .expect("local_path_for_key should be Some");
    assert!(path.ends_with("some/object.bin"));
}

#[test]
fn local_store_put_overwrite_then_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ServerObjectStore::local(dir.path().join("rw")).expect("create local store");

    let key = ObjectKey::parse("rw/data.txt").expect("valid key");
    let original = b"version-1";
    let integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(original),
        u64::try_from(original.len()).unwrap_or(0),
    );
    store
        .put_if_absent(&key, ObjectBody::Borrowed(original), &integrity)
        .expect("put version 1");

    let updated = b"version-2-longer";
    let updated_integrity = ObjectIntegrity::new(
        shardline_server_core::chunk_hash(updated),
        u64::try_from(updated.len()).unwrap_or(0),
    );
    store
        .put_overwrite(&key, ObjectBody::Borrowed(updated), &updated_integrity)
        .expect("put version 2");

    let length = u64::try_from(updated.len()).unwrap_or(0);
    let contents = shardline_server_core::read_full_object(&store, &key, length)
        .expect("read after overwrite");
    assert_eq!(contents, updated);
}
