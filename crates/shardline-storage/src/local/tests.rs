use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::sync::{LazyLock, Mutex};
#[cfg(unix)]
use std::{io::ErrorKind as IoErrorKind, path::PathBuf};

use shardline_protocol::ByteRange;

use super::{LocalObjectStore, LocalObjectStoreError};
#[cfg(unix)]
use crate::local_fs::set_before_local_write_hook;
use crate::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome,
};

/// Serializes hook-based race tests so they don't clobber each other's global hook state.
#[cfg(unix)]
static HOOK_TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn local_object_store_roundtrips_metadata_ranges_inventory_and_delete() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf());
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let body = b"abcdefgh";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 8);

    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    let duplicate = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
    assert!(matches!(duplicate, Ok(PutOutcome::AlreadyExists)));
    assert!(matches!(store.contains(&key), Ok(true)));
    let metadata = store.metadata(&key);
    assert!(matches!(metadata, Ok(Some(_))));
    if let Ok(Some(metadata)) = metadata {
        assert_eq!(metadata.length(), 8);
    }

    let range = ByteRange::new(2, 5);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let read = store.read_range(&key, range);
    assert!(read.is_ok());
    if let Ok(read) = read {
        assert_eq!(read, b"cdef".to_vec());
    }

    let prefix = ObjectPrefix::parse("xorbs/default/");
    assert!(prefix.is_ok());
    let Ok(prefix) = prefix else {
        return;
    };
    let listed = store.list_prefix(&prefix);
    assert!(listed.is_ok());
    let Ok(listed) = listed else {
        return;
    };
    assert_eq!(listed.len(), 1);
    let first = listed.first();
    assert!(first.is_some());
    if let Some(first) = first {
        assert_eq!(first.key(), &key);
    }
    let mut visited = Vec::new();
    let visit = store.visit_prefix(&prefix, |visited_metadata| {
        visited.push(visited_metadata.key().clone());
        Ok::<(), LocalObjectStoreError>(())
    });
    assert!(visit.is_ok());
    assert_eq!(visited, vec![key.clone()]);

    assert!(matches!(
        store.delete_if_present(&key),
        Ok(DeleteOutcome::Deleted)
    ));
    assert!(matches!(
        store.delete_if_present(&key),
        Ok(DeleteOutcome::NotFound)
    ));
    assert!(matches!(store.contains(&key), Ok(false)));
}

#[test]
fn local_object_store_rejects_integrity_mismatch_and_out_of_bounds_range() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf());
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/bb/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let wrong_integrity = ObjectIntegrity::new(super::util::chunk_hash(b"abc"), 4);

    let result = store.put_if_absent(&key, ObjectBody::from_slice(b"abc"), &wrong_integrity);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));

    let integrity = ObjectIntegrity::new(super::util::chunk_hash(b"abc"), 3);
    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(b"abc"), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));

    let range = ByteRange::new(1, 5);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let read = store.read_range(&key, range);
    assert!(matches!(read, Err(LocalObjectStoreError::RangeOutOfBounds)));
}

#[test]
fn local_object_store_installs_temporary_file_without_buffering() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let temporary = storage.path().join("body.tmp");
    let write = fs::write(&temporary, b"streamed-body");
    assert!(write.is_ok());
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(b"streamed-body"), 13);

    let inserted = store.put_temporary_file_if_absent(&key, &temporary, &integrity);

    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
    assert!(!temporary.exists());
    let range = ByteRange::new(0, 12);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let read = store.read_range(&key, range);
    assert!(matches!(read, Ok(bytes) if bytes == b"streamed-body"));
}

#[test]
fn local_object_store_temporary_file_duplicate_is_idempotent() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let key = ObjectKey::parse("xorbs/default/ee/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let body = b"same-body";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 9);
    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
    let duplicate = storage.path().join("duplicate.tmp");
    let write = fs::write(&duplicate, body);
    assert!(write.is_ok());

    let outcome = store.put_temporary_file_if_absent(&key, &duplicate, &integrity);

    assert!(matches!(outcome, Ok(PutOutcome::AlreadyExists)));
    assert!(!duplicate.exists());
}

#[test]
fn local_object_store_temporary_file_rejects_conflicting_existing_object() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let key = ObjectKey::parse("xorbs/default/ef/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let inserted = store.put_if_absent(
        &key,
        ObjectBody::from_slice(b"same-size"),
        &ObjectIntegrity::new(super::util::chunk_hash(b"same-size"), 9),
    );
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));
    let duplicate = storage.path().join("conflict.tmp");
    let write = fs::write(&duplicate, b"other-val");
    assert!(write.is_ok());

    let outcome = store.put_temporary_file_if_absent(
        &key,
        &duplicate,
        &ObjectIntegrity::new(super::util::chunk_hash(b"other-val"), 9),
    );

    assert!(matches!(
        outcome,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));
    assert!(!duplicate.exists());
}

#[test]
fn local_object_store_open_is_non_mutating_until_write() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());

    assert!(!root.exists());

    let key = ObjectKey::parse("xorbs/default/cc/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let contains = store.contains(&key);

    assert!(matches!(contains, Ok(false)));
    assert!(!root.exists());
}

#[cfg(unix)]
#[test]
fn local_object_store_new_rejects_symlinked_root_ancestor() {
    let storage = shardline_test_support::TempStorage::new();
    let target = storage.path().join("target");
    let created = fs::create_dir_all(&target);
    assert!(created.is_ok());
    let link = storage.path().join("link");
    let linked = symlink(&target, &link);
    assert!(linked.is_ok());

    let store = LocalObjectStore::new(link.join("objects"));

    assert!(matches!(
        store,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
}

#[cfg(unix)]
#[test]
fn local_object_store_list_prefix_rejects_symlinked_root_ancestor() {
    let storage = shardline_test_support::TempStorage::new();
    let target = storage.path().join("target");
    let object_root = target.join("objects");
    let parent = object_root.join("aa");
    let created = fs::create_dir_all(&parent);
    assert!(created.is_ok());
    let path = parent.join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let written = fs::write(&path, b"payload");
    assert!(written.is_ok());
    let link = storage.path().join("link");
    let linked = symlink(&target, &link);
    assert!(linked.is_ok());
    let store = LocalObjectStore::open(link.join("objects"));
    let prefix = ObjectPrefix::parse("aa/");
    assert!(prefix.is_ok());
    let Ok(prefix) = prefix else {
        return;
    };

    let listed = store.list_prefix(&prefix);

    assert!(matches!(
        listed,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
}

#[test]
fn local_object_store_maps_validated_key_under_root() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());
    let key = ObjectKey::parse("aa/hash");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };

    let path = store.path_for_key(&key);

    assert_eq!(path, root.join("aa/hash"));
}

#[test]
fn local_object_store_rejects_corrupted_existing_object_under_same_key() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let path = store.path_for_key(&key);
    let parent = path.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());

    let corrupted = fs::write(&path, b"baad");
    assert!(corrupted.is_ok());

    let body = b"good";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 4);
    let outcome = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

    assert!(matches!(
        outcome,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn local_object_store_put_if_absent_with_symlink_target_returns_error() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();

    let key = ObjectKey::parse("xorbs/default/ff/hash.xorb").unwrap();
    let path = store.path_for_key(&key);
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();

    // Create a regular file first, then replace it with a symlink
    let outside = storage.path().join("outside-target");
    std::fs::write(&outside, b"real data").unwrap();
    std::fs::write(&path, b"existing").unwrap();
    std::fs::remove_file(&path).unwrap();
    symlink(&outside, &path).unwrap();

    let body = b"new data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 8);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    // Should fail because existing path is a symlink
    assert!(result.is_err());
}

#[cfg(unix)]
#[test]
fn local_object_store_rejects_symlinked_object_path() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let outside = storage.path().join("outside-secret");
    let outside_write = fs::write(&outside, b"secret");
    assert!(outside_write.is_ok());

    let key = ObjectKey::parse("xorbs/default/ff/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let path = store.path_for_key(&key);
    let parent = path.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let linked = symlink(&outside, &path);
    assert!(linked.is_ok());

    let metadata = store.metadata(&key);
    let range = ByteRange::new(0, 5);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let read = store.read_range(&key, range);

    assert!(matches!(
        metadata,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
    assert!(matches!(
        read,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
}

#[cfg(unix)]
#[test]
fn local_object_store_rejects_symlinked_parent_directory_reads() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let outside = tempfile::tempdir();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let outside_path = outside.path().join("hash.xorb");
    let outside_write = fs::write(&outside_path, b"secret");
    assert!(outside_write.is_ok());

    let link = storage
        .path()
        .join("objects")
        .join("xorbs")
        .join("default")
        .join("aa");
    let parent = link.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());

    let metadata = store.metadata(&key);
    let range = ByteRange::new(0, 5);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let read = store.read_range(&key, range);

    assert!(matches!(
        metadata,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
    assert!(matches!(
        read,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
}

#[cfg(unix)]
#[test]
fn local_object_store_rejects_symlinked_parent_directory_writes() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/bb/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let outside = tempfile::tempdir();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let link = storage
        .path()
        .join("objects")
        .join("xorbs")
        .join("default")
        .join("bb");
    let parent = link.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());

    let body = b"payload";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 7);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

    assert!(matches!(
        result,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
    assert!(
        !outside.path().join("hash.xorb").exists(),
        "object write escaped into a symlink target outside the object root"
    );
}

#[cfg(unix)]
#[test]
fn local_object_store_put_overwrite_rejects_parent_swap_race() {
    let _guard = HOOK_TEST_MUTEX.lock().unwrap();
    let storage = shardline_test_support::TempStorage::new();
    let outside = tempfile::tempdir();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/ee/hash.xorb").unwrap();
    let path = store.path_for_key(&key);
    let parent = path.parent().map(PathBuf::from).unwrap();
    let moved_parent = storage.path().join("swapped-overwrite-parent");
    let escape_dir = outside.path().to_path_buf();

    set_before_local_write_hook(path, move || {
        let renamed = fs::rename(&parent, &moved_parent);
        assert!(renamed.is_ok());
        let linked = symlink(&escape_dir, &parent);
        assert!(linked.is_ok());
    });

    let body = b"payload for overwrite";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 21);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);

    assert!(matches!(
        result,
        Err(LocalObjectStoreError::Io(error)) if error.kind() == IoErrorKind::InvalidData
    ));
    assert!(
        !outside.path().join("hash.xorb").exists(),
        "object write escaped into an attacker-controlled symlink target"
    );
}

#[cfg(unix)]
#[test]
fn local_object_store_rejects_parent_swap_race() {
    let _guard = HOOK_TEST_MUTEX.lock().unwrap();
    let storage = shardline_test_support::TempStorage::new();
    let outside = tempfile::tempdir();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/dd/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let path = store.path_for_key(&key);
    let parent = path.parent().map(PathBuf::from);
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let moved_parent = storage.path().join("swapped-object-parent");
    let moved_parent_for_hook = moved_parent.clone();
    let escape_dir = outside.path().to_path_buf();

    set_before_local_write_hook(path, move || {
        let renamed = fs::rename(&parent, &moved_parent_for_hook);
        assert!(renamed.is_ok());
        let linked = symlink(&escape_dir, &parent);
        assert!(linked.is_ok());
    });

    let body = b"payload";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 7);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);

    assert!(matches!(
        result,
        Err(LocalObjectStoreError::Io(error)) if error.kind() == IoErrorKind::InvalidData
    ));
    assert!(
        !outside.path().join("hash.xorb").exists(),
        "object write escaped into an attacker-controlled symlink target"
    );
    assert!(
        !moved_parent.join("hash.xorb").exists(),
        "object write left a committed file behind in the detached original directory"
    );
}

#[cfg(unix)]
#[test]
fn local_object_store_rejects_symlinked_parent_directory_deletes() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects"));
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let key = ObjectKey::parse("xorbs/default/cc/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let outside = tempfile::tempdir();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let outside_path = outside.path().join("hash.xorb");
    let outside_write = fs::write(&outside_path, b"secret");
    assert!(outside_write.is_ok());

    let link = storage
        .path()
        .join("objects")
        .join("xorbs")
        .join("default")
        .join("cc");
    let parent = link.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());

    let result = store.delete_if_present(&key);

    assert!(matches!(
        result,
        Err(LocalObjectStoreError::InvalidObjectPath)
    ));
    assert!(
        outside_path.exists(),
        "object delete escaped into a symlink target outside the object root"
    );
}

#[test]
fn storage_integrity_download_side_hash_verification() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let body = b"hello world";
    let key = ObjectKey::parse("xorbs/default/11/hash.xorb").unwrap();
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 11);

    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));

    // Full range read
    let range = ByteRange::new(0, 10).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, b"hello world");

    // Partial range read
    let range = ByteRange::new(6, 10).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, b"world");
}

#[test]
fn storage_integrity_put_if_absent_rejects_conflicting_bytes_and_wrong_hash() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let key = ObjectKey::parse("xorbs/default/22/hash.xorb").unwrap();

    // Store original data
    let body = b"original data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 13);
    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));

    // Try to store SAME key with SAME bytes but WRONG hash — should fail
    let wrong_hash_integrity = ObjectIntegrity::new(super::util::chunk_hash(b"other"), 13);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &wrong_hash_integrity);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));

    // Try to store SAME key with CORRECT hash but WRONG length — should fail
    let wrong_length_integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 100);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &wrong_length_integrity);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));

    // Verify original data is intact
    let range = ByteRange::new(0, 12).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, b"original data");
}

#[test]
fn storage_integrity_put_overwrite_rejects_wrong_hash_and_length() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let key = ObjectKey::parse("xorbs/default/33/hash.xorb").unwrap();

    // Store with correct integrity
    let body = b"correct data!!";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 14);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);
    assert!(result.is_ok());

    // Try put_overwrite with wrong hash
    let wrong_hash = ObjectIntegrity::new(super::util::chunk_hash(b"different"), 14);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &wrong_hash);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));

    // Try put_overwrite with wrong length
    let wrong_length = ObjectIntegrity::new(super::util::chunk_hash(body), 999);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &wrong_length);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));

    // Verify original data still intact
    let range = ByteRange::new(0, 13).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, b"correct data!!");
}

#[test]
fn storage_integrity_cross_key_independence() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let key_a = ObjectKey::parse("xorbs/default/aa/key_a.xorb").unwrap();
    let key_b = ObjectKey::parse("xorbs/default/bb/key_b.xorb").unwrap();

    let body_a = b"alpha data";
    let body_b = b"bravo data";
    let integrity_a = ObjectIntegrity::new(super::util::chunk_hash(body_a), 10);
    let integrity_b = ObjectIntegrity::new(super::util::chunk_hash(body_b), 10);

    store
        .put_if_absent(&key_a, ObjectBody::from_slice(body_a), &integrity_a)
        .unwrap();
    store
        .put_if_absent(&key_b, ObjectBody::from_slice(body_b), &integrity_b)
        .unwrap();

    // Verify each returns its own bytes
    let range = ByteRange::new(0, 9).unwrap();
    assert_eq!(store.read_range(&key_a, range).unwrap(), b"alpha data");

    let range = ByteRange::new(0, 9).unwrap();
    assert_eq!(store.read_range(&key_b, range).unwrap(), b"bravo data");

    // Delete one
    assert!(matches!(
        store.delete_if_present(&key_a),
        Ok(DeleteOutcome::Deleted)
    ));

    // Verify the other still exists
    assert!(store.contains(&key_b).unwrap());
    let range = ByteRange::new(0, 9).unwrap();
    assert_eq!(store.read_range(&key_b, range).unwrap(), b"bravo data");
}

#[test]
fn storage_integrity_large_object_round_trip() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    // Create a 1MB object with a repeating pattern
    let pattern: Vec<u8> = (0..=255).collect();
    let body: Vec<u8> = pattern.iter().copied().cycle().take(1024 * 1024).collect();
    assert_eq!(body.len(), 1024 * 1024);

    let key = ObjectKey::parse("xorbs/default/cc/large.xorb").unwrap();
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(&body), body.len() as u64);

    let inserted = store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    assert!(matches!(inserted, Ok(PutOutcome::Inserted)));

    // Full range read
    let range = ByteRange::new(0, body.len() as u64 - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, body);

    // Partial range: early
    let range = ByteRange::new(1000, 1999).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, &body[1000..2000]);

    // Partial range: middle
    let range = ByteRange::new(500_000, 500_999).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, &body[500_000..501_000]);

    // Partial range: last byte
    let range = ByteRange::new(1_048_575, 1_048_575).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0], body[1_048_575]);
}

#[test]
fn local_object_store_copies_existing_object() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let source = ObjectKey::parse("ab/source").unwrap();
    let dest = ObjectKey::parse("cd/dest").unwrap();
    let body = b"hello world";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 11);

    // Put source
    assert!(matches!(
        store.put_if_absent(&source, ObjectBody::from_slice(body), &integrity),
        Ok(PutOutcome::Inserted)
    ));
    // Copy to dest
    assert!(matches!(
        store.copy_object_if_absent(&source, &dest),
        Ok(PutOutcome::Inserted)
    ));
    // Second copy is idempotent
    assert!(matches!(
        store.copy_object_if_absent(&source, &dest),
        Ok(PutOutcome::AlreadyExists)
    ));
    // Dest is readable and matches
    let dest_data = store
        .read_range(&dest, ByteRange::new(0, 10).unwrap())
        .unwrap();
    assert_eq!(dest_data, body);
}

#[test]
fn local_object_store_copy_returns_not_found_for_missing_source() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let source = ObjectKey::parse("ab/missing").unwrap();
    let dest = ObjectKey::parse("cd/dest").unwrap();
    let result = store.copy_object_if_absent(&source, &dest);
    assert!(result.is_err());
}

#[test]
fn local_object_store_copy_source_equals_destination_returns_already_exists() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("ab/self").unwrap();
    let body = b"self copy data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 14);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let result = store.copy_object_if_absent(&key, &key);
    assert!(matches!(result, Ok(PutOutcome::AlreadyExists)));
}

#[test]
fn local_object_store_copy_source_equals_destination_reports_not_found_when_missing() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("ab/self-missing").unwrap();

    let result = store.copy_object_if_absent(&key, &key);
    assert!(result.is_err());
}

#[test]
fn local_object_store_put_overwrite_replaces_existing_content() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("ab/overwrite").unwrap();
    let original = b"original content";
    let replacement = b"replacement content";

    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(original),
            &ObjectIntegrity::new(super::util::chunk_hash(original), original.len() as u64),
        )
        .unwrap();
    store
        .put_overwrite(
            &key,
            ObjectBody::from_slice(replacement),
            &ObjectIntegrity::new(
                super::util::chunk_hash(replacement),
                replacement.len() as u64,
            ),
        )
        .unwrap();

    let data = store
        .read_range(
            &key,
            ByteRange::new(0, replacement.len() as u64 - 1).unwrap(),
        )
        .unwrap();
    assert_eq!(data, replacement);
}

#[test]
fn local_object_store_list_flat_namespace_page_returns_requested_page() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let prefix = ObjectPrefix::parse("ab/").unwrap();
    let key_a = ObjectKey::parse("ab/aaaa").unwrap();
    let key_b = ObjectKey::parse("ab/bbbb").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 4);

    store
        .put_if_absent(&key_a, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_b, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // List with no start_after returns both
    let page = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(page.len(), 2);

    // List with start_after returns remaining
    let page = store
        .list_flat_namespace_page(&prefix, Some(&key_a), 10)
        .unwrap();
    assert_eq!(page.len(), 1);
}

#[test]
fn local_object_store_list_flat_namespace_rejects_start_after_outside_prefix() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let prefix = ObjectPrefix::parse("ab/").unwrap();
    let key = ObjectKey::parse("cd/outsider").unwrap();
    let result = store.list_flat_namespace_page(&prefix, Some(&key), 10);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::InvalidStartAfter)
    ));
}

#[test]
fn local_object_store_open_missing_file_returns_error() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("ab/missing").unwrap();
    let result = store.open_object_file(&key);
    assert!(result.is_err());
}

// ── root() accessor ─────────────────────────────────────────────────────

#[test]
fn local_object_store_root_accessor() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let root = store.root();
    assert_eq!(root, storage.path());
}

#[test]
fn local_object_store_open_root_accessor() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());
    assert_eq!(store.root(), root);
}

// ── list_flat_namespace_page with non-existent prefix ────────────────────

#[test]
fn local_object_store_list_flat_namespace_empty_prefix_returns_empty() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let prefix = ObjectPrefix::parse("zz/").unwrap();
    let result = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert!(result.is_empty());
}

// ── open_object_file successfully reads existing object ──────────────────

#[test]
fn local_object_store_open_object_file_reads_content() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("ab/readable").unwrap();
    let body = b"data for file open";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), body.len() as u64);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let mut file = store.open_object_file(&key).unwrap();
    use std::io::Read;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, body);
}

// ── put_temporary_file_if_absent rejects wrong length ────────────────────

#[test]
fn local_object_store_temporary_file_wrong_length_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("ab/wrong-len-tmp").unwrap();
    let tmp = storage.path().join("wrong.tmp");
    std::fs::write(&tmp, b"actual-body").unwrap();
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(b"actual-body"), 999);
    let result = store.put_temporary_file_if_absent(&key, &tmp, &integrity);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));
    assert!(
        tmp.exists(),
        "temporary file should not be removed on rejected integrity"
    );
}

// ── put_temporary_file_if_absent rejects wrong hash ─────────────────────

#[test]
fn local_object_store_temporary_file_wrong_hash_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("ab/wrong-hash-tmp").unwrap();
    let tmp = storage.path().join("wrong-hash.tmp");
    std::fs::write(&tmp, b"actual-body").unwrap();
    let wrong_hash = super::util::chunk_hash(b"different-body");
    let integrity = ObjectIntegrity::new(wrong_hash, 11);
    let result = store.put_temporary_file_if_absent(&key, &tmp, &integrity);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));
    assert!(tmp.exists());
}

// ── put_temporary_file_if_absent with non-existent parent dir ------------

#[test]
fn local_object_store_temporary_file_link_error_retains_temp() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("ab/integrity-mismatch").unwrap();
    // Use a file whose integrity doesn't match what we claim — the verify step fails
    let tmp = storage.path().join("mismatch.tmp");
    std::fs::write(&tmp, b"actual-content").unwrap();
    let wrong_hash = super::util::chunk_hash(b"different-content");
    let integrity = ObjectIntegrity::new(wrong_hash, 4);
    let result = store.put_temporary_file_if_absent(&key, &tmp, &integrity);
    // Integrity verification should fail (length and hash both wrong)
    assert!(result.is_err());
    // Temp file should still exist after error
    assert!(tmp.exists());
}

// ── path_for_key with various keys ──────────────────────────────────────

#[test]
fn local_object_store_path_for_key_deeply_nested() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());
    let key = ObjectKey::parse("a/b/c/d/e/f/g/h/file.xorb").unwrap();
    let path = store.path_for_key(&key);
    assert_eq!(path, root.join("a/b/c/d/e/f/g/h/file.xorb"));
}

#[test]
fn local_object_store_path_for_key_single_component() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());
    let key = ObjectKey::parse("file.xorb").unwrap();
    let path = store.path_for_key(&key);
    assert_eq!(path, root.join("file.xorb"));
}

#[test]
fn local_object_store_path_for_key_with_dashes_and_underscores() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::open(root.clone());
    let key = ObjectKey::parse("my-prefix/my_file-v2.xorb").unwrap();
    let path = store.path_for_key(&key);
    assert_eq!(path, root.join("my-prefix/my_file-v2.xorb"));
}

// ── put_if_absent error on directory creation failure ───────────────────

#[test]
fn local_object_store_put_if_absent_fails_when_root_is_file_not_directory() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("file-instead-of-dir");
    // Create a file at the root location
    std::fs::write(&root, b"not a directory").unwrap();
    let store = LocalObjectStore::open(root);
    let key = ObjectKey::parse("some/key").unwrap();
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(b"data"), 4);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity);
    assert!(result.is_err());
}

// ── LocalObjectStoreError Display ───────────────────────────────────────

#[test]
fn local_store_error_display_io() {
    let err = LocalObjectStoreError::Io(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "nope",
    ));
    assert!(
        err.to_string()
            .contains("local object store operation failed")
    );
}

#[test]
fn local_store_error_display_integrity_length_mismatch() {
    let err = LocalObjectStoreError::IntegrityLengthMismatch;
    assert_eq!(
        err.to_string(),
        "object body length did not match expected integrity"
    );
}

#[test]
fn local_store_error_display_integrity_hash_mismatch() {
    let err = LocalObjectStoreError::IntegrityHashMismatch;
    assert_eq!(
        err.to_string(),
        "object body hash did not match expected integrity"
    );
}

#[test]
fn local_store_error_display_existing_object_conflict() {
    let err = LocalObjectStoreError::ExistingObjectConflict;
    assert_eq!(
        err.to_string(),
        "object key already exists with conflicting bytes"
    );
}

#[test]
fn local_store_error_display_range_out_of_bounds() {
    let err = LocalObjectStoreError::RangeOutOfBounds;
    assert_eq!(
        err.to_string(),
        "requested byte range exceeded stored object length"
    );
}

#[test]
fn local_store_error_display_invalid_stored_key() {
    let err = LocalObjectStoreError::InvalidStoredKey;
    assert_eq!(
        err.to_string(),
        "stored object path could not be represented as a valid object key"
    );
}

#[test]
fn local_store_error_display_invalid_object_path() {
    let err = LocalObjectStoreError::InvalidObjectPath;
    assert_eq!(
        err.to_string(),
        "validated object key could not be mapped to a local path"
    );
}

#[test]
fn local_store_error_display_invalid_start_after() {
    let err = LocalObjectStoreError::InvalidStartAfter;
    assert_eq!(
        err.to_string(),
        "start_after key is outside the requested prefix"
    );
}

// ── chunk_hash ───────────────────────────────────────────────────────────

#[test]
fn local_chunk_hash_consistent() {
    let a = super::util::chunk_hash(b"test data");
    let b = super::util::chunk_hash(b"test data");
    assert_eq!(a, b);
}

#[test]
fn local_chunk_hash_differs_for_different_inputs() {
    let a = super::util::chunk_hash(b"abc");
    let b = super::util::chunk_hash(b"xyz");
    assert_ne!(a, b);
}

// ── verify_integrity locally ─────────────────────────────────────────────

#[test]
fn local_verify_integrity_accepts_matching() {
    let bytes = b"hello";
    let integrity = crate::ObjectIntegrity::new(super::util::chunk_hash(bytes), 5);
    assert!(super::util::verify_integrity(bytes, &integrity).is_ok());
}

#[test]
fn local_verify_integrity_rejects_length_mismatch() {
    let bytes = b"hello";
    let integrity = crate::ObjectIntegrity::new(super::util::chunk_hash(bytes), 99);
    assert!(matches!(
        super::util::verify_integrity(bytes, &integrity),
        Err(LocalObjectStoreError::IntegrityLengthMismatch)
    ));
}

#[test]
fn local_verify_integrity_rejects_hash_mismatch() {
    let bytes = b"hello";
    let wrong_hash = super::util::chunk_hash(b"wrong");
    let integrity = crate::ObjectIntegrity::new(wrong_hash, 5);
    assert!(matches!(
        super::util::verify_integrity(bytes, &integrity),
        Err(LocalObjectStoreError::IntegrityHashMismatch)
    ));
}

// ── verify_file_integrity directly ───────────────────────────────────────

#[test]
fn local_verify_file_integrity_accepts_correct_file() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("verify.bin");
    std::fs::write(&path, b"file content").unwrap();
    let integrity = crate::ObjectIntegrity::new(super::util::chunk_hash(b"file content"), 12);
    assert!(super::io::verify_file_integrity(&path, &integrity).is_ok());
}

#[test]
fn local_verify_file_integrity_rejects_missing_file() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("notexist.bin");
    let integrity = crate::ObjectIntegrity::new(super::util::chunk_hash(b""), 0);
    let result = super::io::verify_file_integrity(&path, &integrity);
    assert!(result.is_err());
}

// ── object_file_metadata with various paths ──────────────────────────────

#[test]
fn local_object_file_metadata_missing_returns_none() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("ghost.bin");
    let result = super::metadata::object_file_metadata(&path);
    assert!(matches!(result, Ok(None)));
}

#[test]
fn local_object_file_metadata_regular_file_returns_metadata() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("present.bin");
    std::fs::write(&path, b"data").unwrap();
    let result = super::metadata::object_file_metadata(&path);
    assert!(matches!(result, Ok(Some(_))));
}

// ── read_dir_if_exists with non-existent path ────────────────────────────

#[test]
fn local_read_dir_if_exists_missing_returns_none() {
    let storage = shardline_test_support::TempStorage::new();
    let dir = storage.path().join("does/not/exist");
    let result = super::walk::read_dir_if_exists(&dir);
    assert!(matches!(result, Ok(None)));
}

// ─── ensure_file_matches_bytes with length mismatch ──────────────────────

#[test]
fn local_ensure_file_matches_bytes_same_content_ok() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("match-me.bin");
    std::fs::write(&path, b"matching bytes").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let result = super::io::ensure_file_matches_bytes(file, b"matching bytes");
    assert!(result.is_ok());
}

#[test]
fn local_ensure_file_matches_bytes_different_content_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("diff.bin");
    std::fs::write(&path, b"actual content").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let result = super::io::ensure_file_matches_bytes(file, b"expected different");
    assert!(result.is_err());
}

#[test]
fn local_ensure_file_matches_bytes_longer_file_than_expected_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("longer.bin");
    std::fs::write(&path, b"longer file content").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let result = super::io::ensure_file_matches_bytes(file, b"short");
    assert!(result.is_err());
}

// ── ensure_files_match with identical and different files ────────────────

#[test]
fn local_ensure_files_match_identical() {
    let storage = shardline_test_support::TempStorage::new();
    let path_a = storage.path().join("a.bin");
    let path_b = storage.path().join("b.bin");
    std::fs::write(&path_a, b"same data").unwrap();
    std::fs::write(&path_b, b"same data").unwrap();
    let a = std::fs::File::open(&path_a).unwrap();
    let b = std::fs::File::open(&path_b).unwrap();
    let result = super::io::ensure_files_match(a, b);
    assert!(result.is_ok());
}

#[test]
fn local_ensure_files_match_different_sizes_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let path_a = storage.path().join("long.bin");
    let path_b = storage.path().join("short.bin");
    std::fs::write(&path_a, b"longer content").unwrap();
    std::fs::write(&path_b, b"short").unwrap();
    let a = std::fs::File::open(&path_a).unwrap();
    let b = std::fs::File::open(&path_b).unwrap();
    let result = super::io::ensure_files_match(a, b);
    assert!(result.is_err());
}

#[test]
fn local_ensure_files_match_different_content_same_size_rejected() {
    let storage = shardline_test_support::TempStorage::new();
    let path_a = storage.path().join("x.bin");
    let path_b = storage.path().join("y.bin");
    std::fs::write(&path_a, b"aaaa bbbb").unwrap();
    std::fs::write(&path_b, b"bbbb aaaa").unwrap();
    let a = std::fs::File::open(&path_a).unwrap();
    let b = std::fs::File::open(&path_b).unwrap();
    let result = super::io::ensure_files_match(a, b);
    assert!(result.is_err());
}

// ── map_directory_path_error covers all variants ─────────────────────────

#[test]
fn local_map_directory_path_error_symlink_variant() {
    let result = super::util::map_directory_path_error(
        crate::DirectoryPathError::SymlinkedComponent(PathBuf::from("/link")),
    );
    assert!(matches!(result, LocalObjectStoreError::InvalidObjectPath));
}

#[test]
fn local_map_directory_path_error_non_dir_variant() {
    let result = super::util::map_directory_path_error(
        crate::DirectoryPathError::NonDirectoryComponent(PathBuf::from("/file")),
    );
    assert!(matches!(result, LocalObjectStoreError::InvalidObjectPath));
}

#[test]
fn local_map_directory_path_error_unsupported_prefix_variant() {
    let result =
        super::util::map_directory_path_error(crate::DirectoryPathError::UnsupportedPrefix);
    assert!(matches!(result, LocalObjectStoreError::InvalidObjectPath));
}

#[test]
fn local_map_directory_path_error_io_variant() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    let result = super::util::map_directory_path_error(crate::DirectoryPathError::Io(io_err));
    assert!(matches!(result, LocalObjectStoreError::Io(_)));
}

// ── LocalObjectStoreError Display variants ─────────────────────────────

#[test]
fn local_error_display_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "disk error");
    let err = LocalObjectStoreError::Io(io_err);
    let msg = err.to_string();
    assert!(msg.contains("local object store operation failed"));
}

#[test]
fn local_error_display_integrity_length_mismatch() {
    let err = LocalObjectStoreError::IntegrityLengthMismatch;
    assert_eq!(
        err.to_string(),
        "object body length did not match expected integrity"
    );
}

#[test]
fn local_error_display_integrity_hash_mismatch() {
    let err = LocalObjectStoreError::IntegrityHashMismatch;
    assert_eq!(
        err.to_string(),
        "object body hash did not match expected integrity"
    );
}

#[test]
fn local_error_display_existing_object_conflict() {
    let err = LocalObjectStoreError::ExistingObjectConflict;
    assert_eq!(
        err.to_string(),
        "object key already exists with conflicting bytes"
    );
}

#[test]
fn local_error_display_range_out_of_bounds() {
    let err = LocalObjectStoreError::RangeOutOfBounds;
    assert_eq!(
        err.to_string(),
        "requested byte range exceeded stored object length"
    );
}

#[test]
fn local_error_display_invalid_stored_key() {
    let err = LocalObjectStoreError::InvalidStoredKey;
    assert_eq!(
        err.to_string(),
        "stored object path could not be represented as a valid object key"
    );
}

#[test]
fn local_error_display_invalid_object_path() {
    let err = LocalObjectStoreError::InvalidObjectPath;
    assert_eq!(
        err.to_string(),
        "validated object key could not be mapped to a local path"
    );
}

#[test]
fn local_error_display_invalid_start_after() {
    let err = LocalObjectStoreError::InvalidStartAfter;
    assert_eq!(
        err.to_string(),
        "start_after key is outside the requested prefix"
    );
}

#[test]
fn local_error_source_io() {
    use std::error::Error;
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "test");
    let err = LocalObjectStoreError::Io(io_err);
    let source = Error::source(&err);
    assert!(source.is_some());
}

#[test]
fn local_error_source_other_variants_return_none() {
    use std::error::Error;
    assert!(Error::source(&LocalObjectStoreError::IntegrityLengthMismatch).is_none());
    assert!(Error::source(&LocalObjectStoreError::IntegrityHashMismatch).is_none());
    assert!(Error::source(&LocalObjectStoreError::ExistingObjectConflict).is_none());
    assert!(Error::source(&LocalObjectStoreError::RangeOutOfBounds).is_none());
    assert!(Error::source(&LocalObjectStoreError::InvalidStoredKey).is_none());
    assert!(Error::source(&LocalObjectStoreError::InvalidObjectPath).is_none());
    assert!(Error::source(&LocalObjectStoreError::InvalidStartAfter).is_none());
}

// ── list_flat_namespace_page with start_after outside prefix ──────────────

#[test]
fn local_list_flat_namespace_rejects_start_after_outside_prefix() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let prefix = ObjectPrefix::parse("ns1/").unwrap();
    let bad_key = ObjectKey::parse("ns2/outside").unwrap();
    let result = store.list_flat_namespace_page(&prefix, Some(&bad_key), 10);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::InvalidStartAfter)
    ));
}

// ── existing_object_outcome with temporary_bytes ──────────────────────────

#[test]
fn local_existing_object_outcome_with_temporary_bytes_matches() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("target.bin");
    std::fs::write(&path, b"exact match").unwrap();
    let temp = storage.path().join("temp.bin");
    std::fs::write(&temp, b"temp data").unwrap();
    let result = super::io::existing_object_outcome(&path, &temp, Some(b"exact match"));
    assert!(result.is_ok());
}

#[test]
fn local_existing_object_outcome_with_temporary_bytes_mismatch() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("target2.bin");
    std::fs::write(&path, b"exact match").unwrap();
    let temp = storage.path().join("temp2.bin");
    std::fs::write(&temp, b"temp data").unwrap();
    let result = super::io::existing_object_outcome(&path, &temp, Some(b"wrong bytes"));
    assert!(result.is_err());
}

#[test]
fn local_existing_object_outcome_without_temporary_bytes_matches() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("target3.bin");
    std::fs::write(&path, b"same content").unwrap();
    let temp = storage.path().join("temp3.bin");
    std::fs::write(&temp, b"same content").unwrap();
    let result = super::io::existing_object_outcome(&path, &temp, None);
    assert!(result.is_ok());
}

#[test]
fn local_existing_object_outcome_without_temporary_bytes_mismatch() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("target4.bin");
    std::fs::write(&path, b"existing data").unwrap();
    let temp = storage.path().join("temp4.bin");
    std::fs::write(&temp, b"different data").unwrap();
    let result = super::io::existing_object_outcome(&path, &temp, None);
    assert!(result.is_err());
}

// ── remove_temporary_file with non-existent file ──────────────────────────

#[test]
fn local_remove_temporary_file_nonexistent_does_not_error() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("ghost.tmp");
    let result = super::io::remove_temporary_file(&path);
    assert!(result.is_ok());
}

#[test]
fn local_remove_temporary_file_existing_succeeds() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("real.tmp");
    std::fs::write(&path, b"data").unwrap();
    assert!(path.exists());
    let result = super::io::remove_temporary_file(&path);
    assert!(result.is_ok());
    assert!(!path.exists());
}

// ── verify_open_file_integrity directly ────────────────────────────────────

#[test]
fn local_verify_open_file_integrity_accepts_correct() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("open_verify.bin");
    std::fs::write(&path, b"verify me").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let integrity = crate::ObjectIntegrity::new(super::util::chunk_hash(b"verify me"), 9);
    assert!(super::io::verify_open_file_integrity(file, &integrity).is_ok());
}

#[test]
fn local_verify_open_file_integrity_rejects_wrong_hash() {
    let storage = shardline_test_support::TempStorage::new();
    let path = storage.path().join("open_verify_bad.bin");
    std::fs::write(&path, b"wrong hash").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let wrong_hash = super::util::chunk_hash(b"different");
    let integrity = crate::ObjectIntegrity::new(wrong_hash, 10);
    assert!(super::io::verify_open_file_integrity(file, &integrity).is_err());
}

// ── remove_empty_ancestors ─────────────────────────────────────────────

#[test]
fn remove_empty_ancestors_cleans_up_empty_dirs() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("a/b/c/d/file.xorb").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 4);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let d_dir = storage.path().join("a/b/c/d");
    assert!(d_dir.exists());

    store.delete_if_present(&key).unwrap();

    // All empty ancestors should be cleaned up
    assert!(!d_dir.exists(), "empty leaf dir should be removed");
    assert!(
        !storage.path().join("a/b/c").exists(),
        "empty parent dir should be removed"
    );
    assert!(
        !storage.path().join("a/b").exists(),
        "empty grandparent dir should be removed"
    );
    // Root-level dir may still exist if other tests create files there
}

#[test]
fn remove_empty_ancestors_does_not_remove_root() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("file.xorb").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 4);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    store.delete_if_present(&key).unwrap();

    // Root should still exist (it's the store root)
    assert!(storage.path().exists());
}

// ── list_flat_namespace_page skips non-file entries ──────────────────────

#[test]
fn local_list_flat_namespace_skips_subdirectories() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    // Create a file and a subdirectory under the prefix
    let file_key = ObjectKey::parse("ns/afile.xorb").unwrap();
    store
        .put_if_absent(
            &file_key,
            ObjectBody::from_slice(b"data"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"data"), 4),
        )
        .unwrap();
    // Create a subdirectory entry via the filesystem
    let dir = storage.path().join("ns").join("subdir");
    std::fs::create_dir_all(&dir).unwrap();
    let prefix = ObjectPrefix::parse("ns/").unwrap();
    let result = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(result.len(), 1, "subdirectory should be skipped");
    assert_eq!(result[0].key().as_str(), "ns/afile.xorb");
}

// ── read_range with overflow range ───────────────────────────────────────

#[test]
fn local_read_range_overflow_len_returns_out_of_bounds() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("test/small.xorb").unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(b"abc"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"abc"), 3),
        )
        .unwrap();
    // ByteRange(0, u64::MAX) is valid, but len() returns None due to overflow
    let range = ByteRange::new(0, u64::MAX).unwrap();
    let result = store.read_range(&key, range);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::RangeOutOfBounds)
    ));
}

// ── read_range past end of file returns RangeOutOfBounds ─────────────────

// ── list_prefix on non-existent root returns empty ───────────────────────

#[test]
fn local_list_prefix_nonexistent_root_returns_empty() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::open(storage.path().join("nonexistent"));
    let prefix = ObjectPrefix::parse("").unwrap();
    let result = store.list_prefix(&prefix);
    assert!(matches!(result, Ok(list) if list.is_empty()));
}

// ── read_dir_if_exists with non-directory path returns error ─────────────

#[test]
fn local_read_dir_if_exists_on_file_returns_error() {
    let storage = shardline_test_support::TempStorage::new();
    let file_path = storage.path().join("a_regular_file");
    std::fs::write(&file_path, b"content").unwrap();
    let result = super::walk::read_dir_if_exists(&file_path);
    assert!(result.is_err());
}

#[test]
fn local_read_range_beyond_file_returns_out_of_bounds() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("test/short.xorb").unwrap();
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(b"abc"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"abc"), 3),
        )
        .unwrap();
    // Request more bytes than the file contains
    let range = ByteRange::new(1, 10).unwrap();
    let result = store.read_range(&key, range);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::RangeOutOfBounds)
    ));
}

#[cfg(unix)]
#[test]
fn local_delete_if_present_parent_without_write_permission_errors() {
    use std::os::unix::fs::PermissionsExt;
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("locked_dir/file.xorb").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), 4);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    // Remove write permission from the parent directory
    let parent = storage.path().join("locked_dir");
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();
    let result = store.delete_if_present(&key);
    // Restore permissions so cleanup works
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(result.is_err());
}

#[cfg(unix)]
#[test]
fn local_read_dir_if_exists_restricted_directory_returns_error() {
    use std::os::unix::fs::PermissionsExt;
    let storage = shardline_test_support::TempStorage::new();
    let dir = storage.path().join("restricted_dir");
    std::fs::create_dir(&dir).unwrap();
    // Remove read permission so read_dir fails with PermissionDenied
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = super::walk::read_dir_if_exists(&dir);
    // Restore permissions so cleanup works
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(result.is_err());
}

#[cfg(unix)]
#[test]
fn local_visit_prefix_skips_non_file_entries() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    // Create a regular file in the prefix
    let file_key = ObjectKey::parse("pfx/regular.xorb").unwrap();
    store
        .put_if_absent(
            &file_key,
            ObjectBody::from_slice(b"data"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"data"), 4),
        )
        .unwrap();

    // Create a FIFO (named pipe) in the prefix directory - not a regular file
    let fifo_path = storage.path().join("pfx/fifo_entry");
    // Use nix::unistd::mkfifo or raw syscall
    let result = std::process::Command::new("mkfifo")
        .arg(&fifo_path)
        .status();
    // It's OK if mkfifo is not available
    if result.map(|s| s.success()).unwrap_or(false) {
        let prefix = ObjectPrefix::parse("pfx/").unwrap();
        let mut visited = Vec::new();
        let result: Result<(), LocalObjectStoreError> = store.visit_prefix(&prefix, |meta| {
            visited.push(meta.key().clone());
            Ok(())
        });
        assert!(result.is_ok());
        assert_eq!(visited.len(), 1, "FIFO entries should be skipped");
        assert_eq!(visited[0].as_str(), "pfx/regular.xorb");
        // Clean up the FIFO
        let _ = std::fs::remove_file(&fifo_path);
    }
}

// ── copy_object_if_absent with hard-link I/O error (permission denied) ──

#[cfg(unix)]
#[test]
fn local_copy_object_if_absent_hard_link_error() {
    use std::os::unix::fs::PermissionsExt;
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let source = ObjectKey::parse("ab/source").unwrap();
    let dest = ObjectKey::parse("cd/dest").unwrap();
    let body = b"test data for hard link error";
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(body), body.len() as u64);
    store
        .put_if_absent(&source, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Pre-create the destination parent directory with read-only permissions
    // so that hard_link (which needs write permission) fails with EACCES.
    let dest_path = store.path_for_key(&dest);
    let parent = dest_path.parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o555)).unwrap();

    let result = store.copy_object_if_absent(&source, &dest);

    // Restore permissions so tempdir cleanup works.
    let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755));
    let _ = std::fs::remove_dir_all(parent);

    assert!(result.is_err(), "expected hard-link error, got {result:?}");
}

// ── read_range with short file (EOF error path) ─────────────────────

#[test]
fn local_read_range_eof_triggers_out_of_bounds() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("test/tiny.xorb").unwrap();
    let body = b"abc";
    store
        .put_if_absent(
            &key,
            ObjectBody::from_slice(body),
            &ObjectIntegrity::new(super::util::chunk_hash(body), body.len() as u64),
        )
        .unwrap();

    // Request a range well past the end of the file
    let range = ByteRange::new(1, 100).unwrap();
    let result = store.read_range(&key, range);
    assert!(matches!(
        result,
        Err(LocalObjectStoreError::RangeOutOfBounds)
    ));
}

// ── metadata on non-existent key returns None ───────────────────────

#[test]
fn local_metadata_returns_none_for_missing() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();
    let key = ObjectKey::parse("test/nonexistent").unwrap();
    let result = store.metadata(&key).unwrap();
    assert!(result.is_none());
}

// ── list_prefix with mixed directory and file entries ───────────────

#[test]
fn local_list_prefix_skips_non_file_entries() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    // Create a file and a directory under the prefix
    let file_key = ObjectKey::parse("mix/afile.xorb").unwrap();
    store
        .put_if_absent(
            &file_key,
            ObjectBody::from_slice(b"data"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"data"), 4),
        )
        .unwrap();
    let dir = storage.path().join("mix").join("subdir");
    std::fs::create_dir_all(&dir).unwrap();

    let prefix = ObjectPrefix::parse("mix/").unwrap();
    let listed = store.list_prefix(&prefix).unwrap();
    assert_eq!(listed.len(), 1, "directory entries should be skipped");
    assert_eq!(listed[0].key().as_str(), "mix/afile.xorb");
}

// ── put_if_absent with AlreadyExists via race (local_fs) ────────────

#[cfg(unix)]
#[test]
fn local_put_if_absent_handles_already_exists_from_local_fs() {
    use std::sync::{Arc, Barrier};

    let storage = Arc::new(shardline_test_support::TempStorage::new());
    let store = Arc::new(LocalObjectStore::new(storage.path().join("objects")).unwrap());
    let key = Arc::new(ObjectKey::parse("race/concurrent.xorb").unwrap());
    let body = b"race data";
    let integrity = Arc::new(ObjectIntegrity::new(
        super::util::chunk_hash(body),
        body.len() as u64,
    ));
    let barrier = Arc::new(Barrier::new(2));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let s = store.clone();
        let k = key.clone();
        let b = ObjectBody::from_slice(body);
        let i = integrity.clone();
        let bar = barrier.clone();
        handles.push(std::thread::spawn(move || {
            bar.wait();
            s.put_if_absent(&k, b, &i)
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.len(), 2);
    let inserts = results
        .iter()
        .filter(|r| matches!(r, Ok(PutOutcome::Inserted)))
        .count();
    let already = results
        .iter()
        .filter(|r| matches!(r, Ok(PutOutcome::AlreadyExists)))
        .count();

    assert_eq!(inserts, 1, "exactly one insert expected");
    assert_eq!(already, 1, "exactly one AlreadyExists expected");
}

// ── ensure_file_matches_bytes with buffer boundary edge case ────────

#[cfg(unix)]
#[test]
fn local_ensure_file_matches_bytes_large_content() {
    use super::io::ensure_file_matches_bytes;
    use std::fs::File;

    let storage = shardline_test_support::TempStorage::new();
    // Create content larger than the 256KB buffer to exercise chunking
    let size = 1024 * 1024; // 1MB
    let content: Vec<u8> = (0_u8..255).cycle().take(size).collect();
    let path = storage.path().join("large_match.bin");
    std::fs::write(&path, &content).unwrap();
    let file = File::open(&path).unwrap();
    assert!(ensure_file_matches_bytes(file, &content).is_ok());
}

// ── ensure_files_match with content larger than buffer ─────────────

#[cfg(unix)]
#[test]
fn local_ensure_files_match_large_content() {
    use super::io::ensure_files_match;
    use std::fs::File;

    let storage = shardline_test_support::TempStorage::new();
    let size = 1024 * 1024;
    let content: Vec<u8> = (0_u8..255).cycle().take(size).collect();
    let path_a = storage.path().join("large_a.bin");
    let path_b = storage.path().join("large_b.bin");
    std::fs::write(&path_a, &content).unwrap();
    std::fs::write(&path_b, &content).unwrap();
    let a = File::open(&path_a).unwrap();
    let b = File::open(&path_b).unwrap();
    assert!(ensure_files_match(a, b).is_ok());
}

// ── put_temporary_file_if_absent with read-only parent dir ─────────

#[cfg(unix)]
#[test]
fn local_put_temporary_file_if_absent_read_only_parent() {
    use std::os::unix::fs::PermissionsExt;

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = LocalObjectStore::new(root).unwrap();
    let key = ObjectKey::parse("readonly/hash.xorb").unwrap();

    // Create a temporary file with matching integrity
    let tmp = storage.path().join("tmp-body.bin");
    std::fs::write(&tmp, b"correct").unwrap();
    let integrity = ObjectIntegrity::new(super::util::chunk_hash(b"correct"), 7);

    // Pre-create the parent directory and remove write permission.
    // open_anchored_target will still open it (O_RDONLY), but
    // hard_link will fail with EACCES because the directory lacks
    // write permission.
    let path = store.path_for_key(&key);
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o555)).unwrap();

    let result = store.put_temporary_file_if_absent(&key, &tmp, &integrity);

    // Restore so cleanup works.
    let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755));

    assert!(
        result.is_err(),
        "expected EACCES error from read-only parent, got {result:?}"
    );
}

// ── list_prefix with FIFO entry (non-file, non-dir) ────────────────

#[cfg(unix)]
#[test]
fn local_list_prefix_skips_non_file_entries_via_fifo() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalObjectStore::new(storage.path_buf()).unwrap();

    let file_key = ObjectKey::parse("fifo_ns/regular.xorb").unwrap();
    store
        .put_if_absent(
            &file_key,
            ObjectBody::from_slice(b"data"),
            &ObjectIntegrity::new(super::util::chunk_hash(b"data"), 4),
        )
        .unwrap();

    let fifo_path = storage.path().join("fifo_ns/fifo_entry");
    let mkfifo_result = std::process::Command::new("mkfifo")
        .arg(&fifo_path)
        .status();

    if mkfifo_result.map(|s| s.success()).unwrap_or(false) {
        let prefix = ObjectPrefix::parse("fifo_ns/").unwrap();
        let listed = store.list_prefix(&prefix).unwrap();
        assert_eq!(listed.len(), 1, "FIFO entries should be skipped");
        assert_eq!(listed[0].key().as_str(), "fifo_ns/regular.xorb");
        let _ = std::fs::remove_file(&fifo_path);
    }
}

// ── remove_empty_ancestors with race ───────────────────────────────

#[cfg(unix)]
#[test]
fn local_remove_empty_ancestors_with_concurrent_delete() {
    use std::sync::{Arc, Barrier};

    let storage = Arc::new(shardline_test_support::TempStorage::new());
    let store = Arc::new(LocalObjectStore::new(storage.path_buf()).unwrap());
    let key_a = Arc::new(ObjectKey::parse("a/b/c/one.xorb").unwrap());
    let key_b = Arc::new(ObjectKey::parse("a/b/c/two.xorb").unwrap());
    let body = b"d";
    let integrity = Arc::new(ObjectIntegrity::new(super::util::chunk_hash(body), 1));
    let barrier = Arc::new(Barrier::new(2));

    // Create two files in the same deep directory structure
    for key in [&key_a, &key_b] {
        store
            .put_if_absent(key, ObjectBody::from_slice(body), &integrity)
            .unwrap();
    }

    // Delete both files concurrently — the first delete cleans up
    // the now-empty directory, the second encounters NotFound.
    let mut handles = Vec::new();
    for k in [key_a, key_b] {
        let s = store.clone();
        let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
            b.wait();
            s.delete_if_present(&k)
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    // The directory should be cleaned up since both files are deleted.
    // At least one delete succeeded, maybe both.
    assert!(
        !storage.path().join("a/b/c").exists(),
        "empty directory should have been removed"
    );
}
