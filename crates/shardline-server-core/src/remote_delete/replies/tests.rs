#![allow(clippy::expect_used, clippy::unwrap_used)]

use shardline_storage::ObjectKey;

use super::read_bounded;
use crate::{
    remote_delete::GuardedRemoteDeleteError,
    tests::{full_object_get_fixture, full_object_get_fixture_with_length},
};

#[test]
fn saved_reply_reads_use_one_full_get_with_the_same_size_boundary() {
    for body in [None, Some(Vec::new()), Some(vec![1; 16]), Some(vec![1; 17])] {
        let expected = body
            .clone()
            .filter(|bytes| !bytes.is_empty() && bytes.len() <= 16);
        let (store, server) = full_object_get_fixture(body);
        let result = read_bounded(&store, &ObjectKey::parse("aa/materialize").unwrap(), 16);
        server.join().unwrap();
        assert_eq!(result.unwrap(), expected);
    }
}

#[test]
fn truncated_saved_reply_is_a_storage_failure() {
    let (store, server) = full_object_get_fixture_with_length(Some(b"short".to_vec()), Some(16));
    let result = read_bounded(&store, &ObjectKey::parse("aa/materialize").unwrap(), 16);
    server.join().unwrap();
    assert!(matches!(
        result,
        Err(GuardedRemoteDeleteError::ObjectStore(_))
    ));
}

#[test]
fn local_saved_reply_size_rules_match_remote_reads() {
    use shardline_storage::{ObjectBody, ObjectIntegrity};

    let root = tempfile::tempdir().unwrap();
    let store = crate::ServerObjectStore::local(root.path()).unwrap();
    let key = ObjectKey::parse("aa/materialize").unwrap();
    assert!(read_bounded(&store, &key, 16).unwrap().is_none());
    for bytes in [Vec::new(), vec![1; 16], vec![1; 17]] {
        let integrity = ObjectIntegrity::new(crate::chunk_hash(&bytes), bytes.len() as u64);
        store
            .put_overwrite(&key, ObjectBody::Borrowed(&bytes), &integrity)
            .unwrap();
        let expected = (!bytes.is_empty() && bytes.len() <= 16).then_some(bytes);
        assert_eq!(read_bounded(&store, &key, 16).unwrap(), expected);
    }
}
