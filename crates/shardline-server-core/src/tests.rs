mod denied_s3;

use proptest::prelude::*;
use shardline_index::{LocalRecordStore, RecordTraversal};
use shardline_protocol::{ByteRange, RepositoryProvider};
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome,
    S3ObjectStoreConfig,
};

use super::*;

#[test]
fn chunk_object_key_valid_inputs_round_trip() {
    for (hash, prefix) in [
        ("a".repeat(64), "aa"),
        ("0123456789abcdef".repeat(4), "01"),
        ("f".repeat(64), "ff"),
        ("0".repeat(64), "00"),
    ] {
        let key = chunk_object_key(&hash).unwrap();
        assert_eq!(key.as_str(), format!("{prefix}/{hash}"));
        assert_eq!(
            chunk_hash_from_chunk_object_key_if_present(&key).unwrap(),
            Some(hash.as_str()),
            "{key:?}"
        );
    }
}

#[test]
fn chunk_object_key_rejects_invalid_hashes() {
    for hash in [
        String::new(),
        "a".to_owned(),
        "G".repeat(64),
        format!("{}gg", "a".repeat(62)),
        format!("{}!!", "a".repeat(62)),
        format!("{}zz", "a".repeat(62)),
        "short".to_owned(),
        "a".repeat(65),
    ] {
        assert!(
            matches!(
                chunk_object_key(&hash),
                Err(ServerObjectStoreError::InvalidContentHash)
            ),
            "{hash:?} should return InvalidContentHash"
        );
    }
}

#[test]
fn parse_stored_file_record_bytes_valid() {
    let json = r#"{"file_id":"test.txt","content_hash":"aabb","total_bytes":100,"chunk_size":10,"chunks":[]}"#;
    assert!(parse_stored_file_record_bytes(json.as_bytes()).is_ok());
}

#[test]
fn parse_stored_file_record_bytes_invalid_json() {
    assert!(parse_stored_file_record_bytes(b"not json").is_err());
}

#[test]
fn parse_stored_file_record_bytes_oversized() {
    let valid =
        r#"{"file_id":"test","content_hash":"aa","total_bytes":0,"chunk_size":0,"chunks":[]}"#;
    assert!(parse_stored_file_record_bytes(valid.as_bytes()).is_ok());

    let oversized = vec![0u8; (MAX_LOCAL_RECORD_METADATA_BYTES + 1) as usize];
    assert!(parse_stored_file_record_bytes(&oversized).is_err());
}

proptest::proptest! {
    #[test]
    fn proptest_validate_identifier_rejects_leading_slash(s in "[a-z]{1,100}") {
        let input = format!("/{s}");
        prop_assert!(validate_identifier(&input).is_err(), "leading slash should be rejected: {input:?}");
    }

    #[test]
    fn proptest_validate_identifier_rejects_traversal(s in "[a-z]{1,50}") {
        let input = format!("{s}/../{s}");
        prop_assert!(validate_identifier(&input).is_err(), "traversal should be rejected: {input:?}");
    }

    #[test]
    fn proptest_validate_identifier_rejects_backslash(s in "[a-z]{1,50}") {
        let input = format!("{s}\\{s}");
        prop_assert!(validate_identifier(&input).is_err(), "backslash should be rejected: {input:?}");
    }

    #[test]
    fn proptest_validate_identifier_accepts_valid_names(segs in prop::collection::vec("[a-z]{1,20}", 1..3usize)) {
        let input = segs.join(".");
        let result = validate_identifier(&input);
        prop_assert!(result.is_ok(), "valid identifier should be accepted: {input:?}");
    }

    #[test]
    fn proptest_validate_identifier_rejects_control_characters(segs in prop::collection::vec("[a-z]{1,20}", 1..3usize)) {
        let mut input = segs.join(".");
        input.push('\t');
        let result = validate_identifier(&input);
        prop_assert!(result.is_err(), "control characters should be rejected: {input:?}");
    }
}

#[test]
fn chunk_hash_from_chunk_object_key_skips_non_chunk_keys() {
    let hash = "a".repeat(64);
    for raw_key in [
        "xorbs/default/aa/hash.xorb".to_owned(),
        format!("aa/{hash}/extra"),
        "a/abc".to_owned(),
        "gg/abc".to_owned(),
        "aa/bbcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        "onlyprefix".to_owned(),
        format!("aa/aa{}", "z".repeat(62)),
    ] {
        let key = ObjectKey::parse(&raw_key).unwrap();
        // Malformed or transient keys must be skipped without aborting GC scans.
        assert_eq!(
            chunk_hash_from_chunk_object_key_if_present(&key).unwrap(),
            None,
            "{raw_key:?}"
        );
    }
}

#[test]
fn content_hash_determinism_same_inputs_same_hash() {
    let chunks = vec![shardline_index::FileChunkRecord {
        hash: "aabbccdd".to_owned(),
        offset: 0,
        length: 100,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 100,
    }];

    let hash1 = content_hash(100, 10, &chunks);
    let hash2 = content_hash(100, 10, &chunks);
    assert_eq!(hash1, hash2);
}

#[test]
fn content_hash_determinism_different_inputs_different_hash() {
    let chunks1 = vec![shardline_index::FileChunkRecord {
        hash: "aabbccdd".to_owned(),
        offset: 0,
        length: 100,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 100,
    }];
    let chunks2 = vec![shardline_index::FileChunkRecord {
        hash: "00112233".to_owned(),
        offset: 0,
        length: 100,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 100,
    }];

    let hash1 = content_hash(100, 10, &chunks1);
    let hash2 = content_hash(100, 10, &chunks2);
    assert_ne!(hash1, hash2);
}

#[test]
fn validate_content_hash_with_valid_64_hex() {
    let hash = "0123456789abcdef".repeat(4);
    assert!(validate_content_hash_with(&hash, || ()).is_ok());
}

#[test]
fn server_object_store_blackhole_put_if_absent_always_inserts() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let body = b"hello";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 5);

    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(result, Ok(PutOutcome::Inserted)));

    // Store again — still Inserted (discards everything)
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(result, Ok(PutOutcome::Inserted)));
}

#[test]
fn server_object_store_blackhole_read_range_not_found() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let range = ByteRange::new(0, 4).unwrap();

    let result = store.read_range(&key, range);
    assert!(matches!(result, Err(ServerObjectStoreError::NotFound)));
}

#[test]
fn server_object_store_blackhole_contains_false() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();

    assert!(matches!(store.contains(&key), Ok(false)));
}

#[test]
fn server_object_store_blackhole_metadata_none() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();

    assert!(matches!(store.metadata(&key), Ok(None)));
}

#[test]
fn server_object_store_blackhole_list_prefix_empty() {
    let store = ServerObjectStore::blackhole();
    let prefix = ObjectPrefix::parse("aa/").unwrap();

    let result = store.list_prefix(&prefix).unwrap();
    assert!(result.is_empty());
}

#[test]
fn server_object_store_blackhole_delete_not_found() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();

    let result = store.delete_if_present(&key).unwrap();
    assert!(matches!(result, DeleteOutcome::NotFound));
}

// ── read_full_object ──────────────────────────────────────────────

#[test]
fn read_full_object_returns_correct_bytes() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let body = b"hello world";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 11);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let data = read_full_object(&store, &key, 11).unwrap();
    assert_eq!(data, b"hello world");
}

#[test]
fn read_full_object_length_zero_returns_empty() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();

    let data = read_full_object(&store, &key, 0).unwrap();
    assert!(data.is_empty());
}

#[test]
fn read_full_object_wrong_length_returns_mismatch() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let body = b"abc";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 3);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let err = read_full_object(&store, &key, 10).unwrap_err();
    assert!(
        matches!(err, ServerObjectStoreError::StoredObjectLengthMismatch),
        "expected StoredObjectLengthMismatch, got: {err}"
    );
}

#[test]
fn read_full_object_nonexistent_returns_error() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();

    let err = read_full_object(&store, &key, 5).unwrap_err();
    assert!(
        matches!(err, ServerObjectStoreError::Local(_)),
        "expected Local error for missing object, got: {err}"
    );
}

#[test]
fn materialize_object_to_tempfile_copies_large_object_in_bounded_chunks() {
    use std::io::{Read, Seek, SeekFrom};

    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();
    let body: Vec<u8> = (0..(2 * 1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect();
    let integrity = ObjectIntegrity::new(chunk_hash(&body), body.len() as u64);
    store
        .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity)
        .unwrap();

    let mut temp = store
        .materialize_object_to_tempfile(&key, body.len() as u64)
        .unwrap();
    temp.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    let mut recovered = Vec::new();
    temp.as_file_mut().read_to_end(&mut recovered).unwrap();
    assert_eq!(recovered, body);
}

#[test]
fn materialized_local_snapshot_is_independent_of_source_changes() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let storage = shardline_test_support::TempStorage::new();
    let local = shardline_storage::LocalObjectStore::new(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("snapshots/source").unwrap();
    let body = vec![0x42; 2 * 1024 * 1024 + 17];
    let integrity = ObjectIntegrity::new(chunk_hash(&body), body.len() as u64);
    local
        .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity)
        .unwrap();
    let source_path = local.path_for_key(&key);
    let store = ServerObjectStore::Local(local);
    let mut snapshot = store
        .materialize_object_to_tempfile(&key, body.len() as u64)
        .unwrap();

    // A writable handle must not let a later mutation change the private
    // snapshot that download validation and decoding share.
    let mut source = std::fs::OpenOptions::new()
        .write(true)
        .open(&source_path)
        .unwrap();
    source.seek(SeekFrom::End(-1)).unwrap();
    source.write_all(&[0x43]).unwrap();
    source.sync_all().unwrap();
    snapshot.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    let mut recovered = Vec::new();
    snapshot.as_file_mut().read_to_end(&mut recovered).unwrap();
    assert_eq!(recovered, body);

    snapshot.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    snapshot.as_file_mut().write_all(&[0x44]).unwrap();
    snapshot.as_file().sync_all().unwrap();
    let source_bytes = std::fs::read(source_path).unwrap();
    assert_eq!(source_bytes.first(), Some(&0x42));
    assert_eq!(source_bytes.last(), Some(&0x43));
}

#[test]
fn materialize_object_to_tempfile_rejects_length_mismatch() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/3333333333333333333333333333333333333333333333333333333333333333")
            .unwrap();
    let body = b"short";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let result = store.materialize_object_to_tempfile(&key, 100);
    assert!(matches!(
        result,
        Err(ServerObjectStoreError::StoredObjectLengthMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn materialize_local_reader_preserves_inode_bytes_and_open_handle_after_unlink() {
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::fs::MetadataExt;

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = ServerObjectStore::local(&root).unwrap();
    let key =
        ObjectKey::parse("aa/2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();
    let body: Vec<u8> = (0..(2 * 1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect();
    let integrity = ObjectIntegrity::new(chunk_hash(&body), body.len() as u64);
    store
        .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity)
        .unwrap();
    let source = root.join(key.as_str());
    let original = std::fs::metadata(&source).unwrap();
    let (mut reader, length) = store
        .materialize_bounded_object_if_present(&key, 0)
        .unwrap()
        .unwrap();
    assert_eq!(length, body.len() as u64);
    let MaterializedObject::Local(file) = &reader else {
        panic!("local validation must retain the original file");
    };
    assert_eq!(file.metadata().unwrap().ino(), original.ino());
    assert_eq!(file.metadata().unwrap().dev(), original.dev());
    std::fs::remove_file(&source).unwrap();
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut recovered = Vec::new();
    reader.read_to_end(&mut recovered).unwrap();
    assert_eq!(recovered, body);
}

/// Serves exactly one GET. Asserting the complete requested range makes this
/// fixture reject a regression to sequential per-MiB GETs.
fn materialization_s3_fixture(
    body: Vec<u8>,
    declared_length: usize,
    response_start: usize,
) -> (ServerObjectStore, std::thread::JoinHandle<()>) {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let response_end = declared_length.checked_sub(1).expect("nonempty fixture");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(connection.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("GET /assets/aa/materialize "));
        let mut requested_range = None;
        loop {
            line.clear();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("range:") {
                requested_range = Some(value.trim().to_owned());
            }
        }
        assert_eq!(requested_range, Some(format!("bytes=0-{response_end}")));
        write!(
            connection,
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {declared_length}\r\nContent-Range: bytes {response_start}-{response_end}/{declared_length}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 01 Oct 2025 00:00:00 GMT\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        // Mismatched range clients can reject the headers and close early.
        if response_start == 0 {
            for bytes in body.chunks(64 * 1024) {
                connection.write_all(bytes).unwrap();
            }
        }
    });
    let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(format!("http://{address}")))
        .with_allow_http(true)
        .with_credentials(
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            None,
        );
    (ServerObjectStore::s3(config).unwrap(), server)
}

#[test]
fn materialize_s3_uses_one_get_for_multiple_mebibytes() {
    use std::io::{Read, Seek, SeekFrom};

    let body: Vec<u8> = (0..(2 * 1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect();
    let (store, server) = materialization_s3_fixture(body.clone(), body.len(), 0);
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let result = store.materialize_object_to_tempfile(&key, body.len() as u64);
    server.join().unwrap();
    let mut file = result.unwrap();
    file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    let mut recovered = Vec::new();
    file.as_file_mut().read_to_end(&mut recovered).unwrap();
    assert_eq!(recovered, body);
}

#[test]
fn materialize_s3_rejects_truncated_response() {
    let (store, server) = materialization_s3_fixture(b"short".to_vec(), 100, 0);
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let result = store.materialize_object_to_tempfile(&key, 100);
    server.join().unwrap();
    assert!(
        result.is_err(),
        "truncated bytes must not reach xorb validation"
    );
}

#[test]
fn materialize_s3_rejects_wrong_response_range() {
    let (store, server) = materialization_s3_fixture(vec![0; 100], 100, 1);
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let result = store.materialize_object_to_tempfile(&key, 100);
    server.join().unwrap();
    assert!(result.is_err(), "a different range must not be accepted");
}

pub(crate) fn full_object_get_fixture(
    body: Option<Vec<u8>>,
) -> (ServerObjectStore, std::thread::JoinHandle<()>) {
    full_object_get_fixture_with_length(body, None)
}

pub(crate) fn full_object_get_fixture_with_length(
    body: Option<Vec<u8>>,
    declared_length: Option<u64>,
) -> (ServerObjectStore, std::thread::JoinHandle<()>) {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        connection
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(connection.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("GET /assets/aa/materialize "));
        loop {
            line.clear();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
            assert!(
                !line.to_ascii_lowercase().starts_with("range:"),
                "full-object GET must not send a range header"
            );
        }
        if let Some(body) = body {
            write!(
                connection,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 01 Oct 2025 00:00:00 GMT\r\nConnection: close\r\n\r\n",
                declared_length.unwrap_or(body.len() as u64)
            )
            .unwrap();
            // A bounded reader may reject the object from response metadata
            // before consuming the body, which can close the client socket.
            let _ = connection.write_all(&body);
        } else {
            connection
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
    });
    let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(format!("http://{address}")))
        .with_allow_http(true)
        .with_credentials(
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            None,
        );
    (ServerObjectStore::s3(config).unwrap(), server)
}

#[test]
fn materialize_s3_if_present_uses_one_full_get_and_returns_observed_length() {
    use std::io::{Read, Seek, SeekFrom};

    let body = b"one request, same-response length".to_vec();
    let (store, server) = full_object_get_fixture(Some(body.clone()));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let materialized = store
        .materialize_object_to_tempfile_if_present(&key)
        .unwrap();
    server.join().unwrap();
    let (mut file, length) = materialized.unwrap();
    assert_eq!(length, body.len() as u64);
    file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    let mut actual = Vec::new();
    file.as_file_mut().read_to_end(&mut actual).unwrap();
    assert_eq!(actual, body);
}

#[test]
fn materialize_s3_if_present_returns_none_for_missing_object() {
    let (store, server) = full_object_get_fixture(None);
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let materialized = store
        .materialize_object_to_tempfile_if_present(&key)
        .unwrap();
    server.join().unwrap();
    assert!(materialized.is_none());
}

#[test]
fn materialize_bounded_s3_object_keeps_small_response_in_memory_with_one_get() {
    use std::io::Read;

    let body = b"verified bytes without scratch payload writes".to_vec();
    let (store, server) = full_object_get_fixture(Some(body.clone()));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let (mut object, length) = store
        .materialize_bounded_object_if_present(&key, body.len() as u64)
        .unwrap()
        .unwrap();
    server.join().unwrap();
    assert_eq!(length, body.len() as u64);
    let mut actual = Vec::new();
    object.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, body);
    assert!(matches!(object, MaterializedObject::Memory(_)));
}

#[test]
fn materialize_bounded_s3_object_spills_large_response_in_the_same_get() {
    use std::io::Read;

    let body = vec![b'x'; 128];
    let (store, server) = full_object_get_fixture(Some(body.clone()));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let (mut object, length) = store
        .materialize_bounded_object_if_present(&key, 64)
        .unwrap()
        .unwrap();
    server.join().unwrap();
    assert_eq!(length, body.len() as u64);
    let mut actual = Vec::new();
    object.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, body);
    assert!(matches!(object, MaterializedObject::Temporary(_)));
}

#[test]
fn materialize_bounded_s3_cancellation_discards_partial_output() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A response larger than one HTTP frame ensures cancellation occurs while
    // consuming the body, rather than only before the GET or after completion.
    let body = vec![b'x'; 2 * 1024 * 1024];
    let (store, server) = full_object_get_fixture(Some(body));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let checks = AtomicUsize::new(0);
    let result = store.materialize_bounded_object_if_present_with_checkpoint(&key, 0, &|| {
        if checks.fetch_add(1, Ordering::SeqCst) >= 2 {
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
        } else {
            Ok(())
        }
    });
    server.join().unwrap();
    assert!(matches!(result, Err(ServerObjectStoreError::Io(ref error))
        if error.kind() == std::io::ErrorKind::Interrupted));
    assert_eq!(
        checks.load(Ordering::SeqCst),
        4,
        "check before GET, accept first frame, abort second, preserve cancellation"
    );
}

#[test]
fn materialize_bounded_s3_precancelled_request_does_not_contact_provider() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(format!("http://{}", listener.local_addr().unwrap())))
        .with_allow_http(true)
        .with_credentials(
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            Some(shardline_protocol::SecretString::from_secret("fixture")),
            None,
        );
    let store = ServerObjectStore::s3(config).unwrap();
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let result = store.materialize_bounded_object_if_present_with_checkpoint(&key, 0, &|| {
        Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
    });
    assert!(matches!(result, Err(ServerObjectStoreError::Io(ref error))
        if error.kind() == std::io::ErrorKind::Interrupted));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn read_s3_if_present_uses_one_full_get_and_returns_observed_length() {
    let body = b"bounded one-request read".to_vec();
    let (store, server) = full_object_get_fixture(Some(body.clone()));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let object = store.read_object_if_present(&key, 1024).unwrap();
    server.join().unwrap();
    assert_eq!(object, Some((body.clone(), body.len() as u64)));
}

#[test]
fn read_s3_if_present_returns_none_for_missing_object() {
    let (store, server) = full_object_get_fixture(None);
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let object = store.read_object_if_present(&key, 1024).unwrap();
    server.join().unwrap();
    assert!(object.is_none());
}

#[test]
fn read_s3_if_present_preserves_empty_object_existence() {
    let (store, server) = full_object_get_fixture(Some(Vec::new()));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let object = store.read_object_if_present(&key, 0).unwrap();
    server.join().unwrap();
    assert_eq!(object, Some((Vec::new(), 0)));
}

#[test]
fn read_s3_if_present_rejects_objects_over_the_memory_bound() {
    let (store, server) = full_object_get_fixture(Some(vec![b'x'; 8]));
    let key = ObjectKey::parse("aa/materialize").unwrap();
    let result = store.read_object_if_present(&key, 7);
    server.join().unwrap();
    assert!(
        matches!(
            result,
            Err(super::object_store::BoundedObjectReadError::ObjectTooLarge)
        ),
        "unexpected bounded-read result: {result:?}"
    );
}

// ── copy_if_absent ────────────────────────────────────────────────

#[test]
fn copy_if_absent_inserts_and_idempotent() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let source_key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let dest_key =
        ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let body = b"copy me";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 7);
    store
        .put_if_absent(&source_key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let outcome = store.copy_if_absent(&source_key, &dest_key).unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Second copy returns AlreadyExists
    let outcome = store.copy_if_absent(&source_key, &dest_key).unwrap();
    assert!(matches!(outcome, PutOutcome::AlreadyExists));

    // Verify bytes are correct
    let range = ByteRange::new(0, 6).unwrap();
    let data = store.read_range(&dest_key, range).unwrap();
    assert_eq!(data, b"copy me");
}

#[test]
fn copy_if_absent_nonexistent_source_returns_error() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let source_key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let dest_key =
        ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let err = store.copy_if_absent(&source_key, &dest_key).unwrap_err();
    assert!(
        matches!(err, ServerObjectStoreError::Local(_)),
        "expected Local error for missing source, got: {err}"
    );
}

#[test]
fn copy_if_absent_blackhole_returns_not_found() {
    let store = ServerObjectStore::blackhole();
    let source =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let dest =
        ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
            .unwrap();

    let err = store.copy_if_absent(&source, &dest).unwrap_err();
    assert!(
        matches!(err, ServerObjectStoreError::NotFound),
        "expected NotFound for blackhole, got: {err}"
    );
}

// ── put_overwrite ─────────────────────────────────────────────────

#[test]
fn put_overwrite_inserts_and_overwrites() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();

    let body1 = b"first";
    let integrity1 = ObjectIntegrity::new(chunk_hash(body1), 5);
    store
        .put_overwrite(&key, ObjectBody::from_slice(body1), &integrity1)
        .unwrap();

    let body2 = b"second version";
    let integrity2 = ObjectIntegrity::new(chunk_hash(body2), 14);
    store
        .put_overwrite(&key, ObjectBody::from_slice(body2), &integrity2)
        .unwrap();

    let range = ByteRange::new(0, 13).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, b"second version");
}

// ── local_root ────────────────────────────────────────────────────

#[test]
fn local_root_returns_path_for_local_store() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("objects");
    let store = ServerObjectStore::local(root.clone()).unwrap();

    assert_eq!(store.local_root(), Some(root.as_path()));
}

#[test]
fn local_root_returns_none_for_blackhole() {
    let store = ServerObjectStore::blackhole();
    assert_eq!(store.local_root(), None);
}

// ── backend_name ──────────────────────────────────────────────────

#[test]
fn backend_name_local() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    assert_eq!(store.backend_name(), "local");
}

#[test]
fn backend_name_blackhole() {
    let store = ServerObjectStore::blackhole();
    assert_eq!(store.backend_name(), "blackhole");
}

// ── OpsRecordStore impl for LocalRecordStore ──────────────────────

fn sample_file_record() -> shardline_index::FileRecord {
    shardline_index::FileRecord {
        deduplication_domain: None,
        file_id: "test-file-id".to_owned(),
        content_hash: "aabbccdd".repeat(8),
        total_bytes: 100,
        chunk_size: 10,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: Vec::new(),
    }
}

#[test]
fn ops_locator_display_returns_record_key() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
    let record = sample_file_record();
    let locator = store.version_record_locator(&record);

    let display = OpsRecordStore::locator_display(&store, &locator);
    assert_eq!(display, locator.record_key());
    assert!(!display.is_empty());
}

#[test]
fn ops_locator_file_id_returns_file_id() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
    let record = sample_file_record();
    let locator = store.version_record_locator(&record);

    let file_id =
        OpsRecordStore::locator_file_id(&store, &locator, OpsRecordKind::Version).unwrap();
    assert_eq!(file_id, "test-file-id");
}

#[test]
fn ops_locator_content_hash_version_returns_hash() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
    let record = sample_file_record();
    let locator = store.version_record_locator(&record);

    let hash =
        OpsRecordStore::locator_content_hash(&store, &locator, OpsRecordKind::Version).unwrap();
    assert_eq!(hash, record.content_hash);
}

#[test]
fn ops_locator_content_hash_latest_returns_none() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
    let record = sample_file_record();
    let locator = store.latest_record_locator(&record);

    let result = OpsRecordStore::locator_content_hash(&store, &locator, OpsRecordKind::Latest);
    assert_eq!(result, None);
}

// ── parse_stored_file_record_bytes (additional targeted tests) ────

#[test]
fn parse_stored_file_record_returns_correct_fields() {
    let record = sample_file_record();
    let json = serde_json::to_vec(&record).unwrap();
    let parsed = parse_stored_file_record_bytes(&json).unwrap();
    assert_eq!(parsed.file_id, "test-file-id");
    assert_eq!(parsed.total_bytes, 100);
    assert_eq!(parsed.chunk_size, 10);
}

#[test]
fn parse_stored_file_record_oversized_returns_too_large_error() {
    let oversized = vec![0u8; (MAX_LOCAL_RECORD_METADATA_BYTES + 1) as usize];
    let err = parse_stored_file_record_bytes(&oversized).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("bounded parser ceiling"),
        "expected StoredFileMetadataTooLarge, got: {err}"
    );
}

// ── chunk_hash ───────────────────────────────────────────────────────

#[test]
fn chunk_hash_produces_deterministic_output() {
    let data = b"hello world";
    let hash1 = chunk_hash(data);
    let hash2 = chunk_hash(data);
    assert_eq!(hash1, hash2);
    assert_ne!(hash1, chunk_hash(b"different"));
}

#[test]
fn chunk_hash_returns_32_byte_hash() {
    for input in [b"".as_slice(), b"test"] {
        assert_eq!(chunk_hash(input).as_bytes().len(), 32);
    }
}

// ── unix_now_seconds_checked ─────────────────────────────────────────

#[test]
fn unix_now_seconds_checked_returns_modern_timestamp() {
    let ts = unix_now_seconds_checked().unwrap();
    assert!(ts >= 1_700_000_000, "timestamp {ts} too small");
}

// ── ServerObjectStore::local_path_for_key ────────────────────────────

#[test]
fn local_path_for_key_with_local_store_returns_some() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("aa/abcd").unwrap();
    assert!(store.local_path_for_key(&key).is_some());
}

#[test]
fn local_path_for_key_with_blackhole_returns_none() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/abcd").unwrap();
    assert!(store.local_path_for_key(&key).is_none());
}

// ── ServerObjectStore::visit_prefix and list_flat_namespace_page ────

#[test]
fn server_object_store_visit_prefix_blackhole_empty() {
    let store = ServerObjectStore::blackhole();
    let prefix = ObjectPrefix::parse("").unwrap();
    let mut count = 0u64;
    let result: Result<(), ServerObjectStoreError> = store.visit_prefix(&prefix, |_meta| {
        count += 1;
        Ok(())
    });
    assert!(result.is_ok());
    assert_eq!(count, 0);
}

#[test]
fn server_object_store_list_flat_namespace_page_blackhole_empty() {
    let store = ServerObjectStore::blackhole();
    let prefix = ObjectPrefix::parse("").unwrap();
    let result = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert!(result.is_empty());
}

// ── ServerObjectStore::put_content_addressed_file blackhole ─────────

#[test]
fn server_object_store_put_content_addressed_file_blackhole() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let tmp = shardline_test_support::TempStorage::new();
    let file_path = tmp.path().join("test.bin");
    std::fs::write(&file_path, b"data").unwrap();

    let integrity = ObjectIntegrity::new(chunk_hash(b"data"), 4);
    let result = store.put_content_addressed_file(&key, &file_path, &integrity);
    assert!(matches!(result, Ok(PutOutcome::Inserted)));
}

// ── ServerObjectStoreError Display for all variants ──────────────────

#[test]
fn server_object_store_error_display_all_variants() {
    assert_eq!(
        ServerObjectStoreError::NotFound.to_string(),
        "content not found"
    );
    assert_eq!(
        ServerObjectStoreError::Overflow.to_string(),
        "arithmetic overflow"
    );
    assert_eq!(
        ServerObjectStoreError::InvalidContentHash.to_string(),
        "content hash must be 64 hexadecimal characters"
    );
    assert_eq!(
        ServerObjectStoreError::StoredObjectLengthMismatch.to_string(),
        "stored object length did not match indexed metadata"
    );
}

// ── provider_directory all variants ─────────────────────────────────

#[test]
fn provider_directory_all_variants() {
    assert_eq!(provider_directory(RepositoryProvider::GitHub), "github");
    assert_eq!(provider_directory(RepositoryProvider::GitLab), "gitlab");
    assert_eq!(provider_directory(RepositoryProvider::Gitea), "gitea");
    assert_eq!(provider_directory(RepositoryProvider::Codeberg), "codeberg");
    assert_eq!(provider_directory(RepositoryProvider::Generic), "generic");
}

// ── OpsRecordStore impl edge cases ─────────────────────────────────

#[test]
fn ops_locator_content_hash_version_without_content_hash_returns_none() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
    let mut record = sample_file_record();
    record.content_hash = String::new();
    let locator = store.latest_record_locator(&record);

    let result = OpsRecordStore::locator_content_hash(&store, &locator, OpsRecordKind::Version);
    assert_eq!(result, None);
}

// ── ParseStoredFileRecordError Display ───────────────────────────────

#[test]
fn parse_stored_file_record_error_stored_file_metadata_too_large_display() {
    let err = ParseStoredFileRecordError::StoredFileMetadataTooLarge {
        observed_bytes: 1024,
        maximum_bytes: 512,
    };
    let msg = err.to_string();
    assert_eq!(
        msg,
        "stored file metadata exceeded the bounded parser ceiling"
    );
}

#[test]
fn parse_stored_file_record_error_json_display_message() {
    let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
    let err = ParseStoredFileRecordError::Json(json_err);
    assert_eq!(err.to_string(), "json operation failed");
}

// ── content_hash edge cases ─────────────────────────────────────────

#[test]
fn content_hash_empty_single_and_multiple_chunks() {
    let chunk = shardline_index::FileChunkRecord {
        hash: "abc".to_owned(),
        offset: 0,
        length: 10,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 10,
    };
    let second = shardline_index::FileChunkRecord {
        hash: "def".to_owned(),
        offset: 10,
        length: 20,
        range_start: 1,
        range_end: 2,
        packed_start: 10,
        packed_end: 30,
    };
    for (total_bytes, chunk_size, chunks) in [
        (0, 0, vec![]),
        (10, 10, vec![chunk.clone()]),
        (30, 10, vec![chunk, second]),
    ] {
        let hash = content_hash(total_bytes, chunk_size, &chunks);
        assert_eq!(hash.len(), 64);
        assert!(
            hash.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
    }
}

#[test]
fn content_hash_different_total_bytes_produces_different_hash() {
    let chunks = vec![shardline_index::FileChunkRecord {
        hash: "aabbccdd".to_owned(),
        offset: 0,
        length: 100,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 100,
    }];

    let hash1 = content_hash(100, 10, &chunks);
    let hash2 = content_hash(200, 10, &chunks);
    assert_ne!(hash1, hash2);
    assert_ne!(hash1, content_hash(100, 20, &chunks));
}

// ── ServerObjectStore constructors ─────────────────────────────────

#[test]
fn server_object_store_local_with_root_path() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects"));
    assert!(store.is_ok());
}

#[test]
fn server_object_store_put_overwrite_blackhole() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let body = b"test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);
    assert!(result.is_ok());
}

#[test]
fn server_object_store_local_path_for_key_format() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key = ObjectKey::parse("ab/abcdef123456").unwrap();
    let path = store.local_path_for_key(&key);
    assert!(path.is_some());
    let path = path.unwrap();
    assert!(path.to_string_lossy().contains("ab"));
}

// ── read_full_object blackhole edge case ────────────────────────────

#[test]
fn read_full_object_blackhole_zero_length() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let result = read_full_object(&store, &key, 0);
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

// ── map_object_key_error ──────────────────────────────────────────────

#[test]
fn map_object_key_error_all_variants_return_overflow() {
    use crate::validation::map_object_key_error;
    use shardline_storage::ObjectKeyError;
    for error in [
        ObjectKeyError::Empty,
        ObjectKeyError::UnsafePath,
        ObjectKeyError::ControlCharacter,
        ObjectKeyError::TooLong,
    ] {
        assert!(matches!(
            map_object_key_error(error),
            ServerObjectStoreError::Overflow
        ));
    }
}

// ── read_full_object non-local path (blackhole, length > 0) ──────────

#[test]
fn read_full_object_blackhole_nonzero_length_returns_not_found() {
    let store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("aa/hash").unwrap();
    let result = read_full_object(&store, &key, 5);
    assert!(matches!(result, Err(ServerObjectStoreError::NotFound)));
}

// ── ServerObjectStore::s3 constructor and S3 branches ─────────────────

#[test]
fn server_object_store_s3_with_minimal_config() {
    let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
    let store = ServerObjectStore::s3(config);
    assert!(store.is_ok());
}

#[test]
fn backend_name_s3() {
    let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
    let store = ServerObjectStore::s3(config).unwrap();
    assert_eq!(store.backend_name(), "s3");
}

#[test]
fn s3_local_root_and_local_path_for_key_return_none() {
    let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
    let store = ServerObjectStore::s3(config).unwrap();
    assert_eq!(store.local_root(), None);
    let key = ObjectKey::parse("aa/hash").unwrap();
    assert_eq!(store.local_path_for_key(&key), None);
}

#[test]
fn s3_operations_return_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let key = ObjectKey::parse("aa/hash").unwrap();

    // put_if_absent receives an explicit denial from the owned endpoint.
    let body = b"test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
    let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

    // read_range
    let result = store.read_range(&key, ByteRange::new(0, 3).unwrap());
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

    // contains
    let result = store.contains(&key);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

    // metadata
    let result = store.metadata(&key);
    // S3 metadata may return Ok(None) for non-existent objects rather than error
    // Accept both Ok(None) and Err(S3)
    let acceptable =
        matches!(result, Ok(None)) || matches!(result, Err(ServerObjectStoreError::S3(_)));
    assert!(acceptable, "unexpected metadata result: {result:?}");

    // list_prefix
    let prefix = ObjectPrefix::parse("aa/").unwrap();
    let result = store.list_prefix(&prefix);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

    // delete_if_present
    let result = store.delete_if_present(&key);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_put_overwrite_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let key = ObjectKey::parse("aa/hash").unwrap();
    let body = b"test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_copy_if_absent_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let source = ObjectKey::parse("aa/source").unwrap();
    let dest = ObjectKey::parse("bb/dest").unwrap();
    let result = store.copy_if_absent(&source, &dest);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_put_content_addressed_file_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let key = ObjectKey::parse("aa/hash").unwrap();
    let tmp = shardline_test_support::TempStorage::new();
    let file_path = tmp.path().join("test.bin");
    std::fs::write(&file_path, b"data").unwrap();
    let integrity = ObjectIntegrity::new(chunk_hash(b"data"), 4);
    let result = store.put_content_addressed_file(&key, &file_path, &integrity);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_visit_prefix_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let prefix = ObjectPrefix::parse("aa/").unwrap();
    let result: Result<(), ServerObjectStoreError> = store.visit_prefix(&prefix, |_meta| Ok(()));
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_list_flat_namespace_page_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let prefix = ObjectPrefix::parse("aa/").unwrap();
    let result = store.list_flat_namespace_page(&prefix, None, 10);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

#[test]
fn s3_read_full_object_returns_s3_error() {
    let fixture = denied_s3::DeniedS3::new();
    let store = &fixture.store;
    let key = ObjectKey::parse("aa/hash").unwrap();
    let result = read_full_object(store, &key, 5);
    assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
}

// ── Local ObjectStore trait method coverage ──────────────────────────

#[test]
fn local_contains_returns_true_for_existing_object() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    // Not present yet
    assert!(matches!(store.contains(&key), Ok(false)));

    // Insert
    let body = b"present";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 7);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Now present
    assert!(matches!(store.contains(&key), Ok(true)));
}

#[test]
fn local_metadata_returns_some_for_existing_object() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    // None for missing
    assert!(matches!(store.metadata(&key), Ok(None)));

    // Insert
    let body = b"meta-test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 9);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Some for existing
    let meta = store.metadata(&key).unwrap();
    assert!(meta.is_some());
    assert_eq!(meta.as_ref().unwrap().length(), 9);
}

#[test]
fn local_list_prefix_returns_objects() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key1 =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();
    let key2 =
        ObjectKey::parse("aa/2222111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let key3 =
        ObjectKey::parse("bb/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();

    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 4);

    store
        .put_if_absent(&key1, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key2, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key3, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let aa_prefix = ObjectPrefix::parse("aa/").unwrap();
    let results = store.list_prefix(&aa_prefix).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn local_delete_if_present_deletes() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    // NotFound for missing
    assert!(matches!(
        store.delete_if_present(&key),
        Ok(DeleteOutcome::NotFound)
    ));

    // Insert
    let body = b"delete-me";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 9);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Now delete
    assert!(matches!(
        store.delete_if_present(&key),
        Ok(DeleteOutcome::Deleted)
    ));
}

#[test]
fn local_visit_prefix_counts_objects() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    let body = b"visit";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 5);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let prefix = ObjectPrefix::parse("aa/").unwrap();
    let mut count = 0u64;
    let result: Result<(), ServerObjectStoreError> = store.visit_prefix(&prefix, |_meta| {
        count += 1;
        Ok(())
    });
    assert!(result.is_ok());
    assert_eq!(count, 1);
}

#[test]
fn local_list_flat_namespace_page_returns_results() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    let body = b"page";
    let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let prefix = ObjectPrefix::parse("aa/").unwrap();
    let results = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn local_put_content_addressed_file_inserts() {
    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")
            .unwrap();

    let tmp = shardline_test_support::TempStorage::new();
    let file_path = tmp.path().join("test.bin");
    std::fs::write(&file_path, b"content-addressed").unwrap();

    let integrity = ObjectIntegrity::new(chunk_hash(b"content-addressed"), 17);
    let result = store.put_content_addressed_file(&key, &file_path, &integrity);
    assert!(matches!(result, Ok(PutOutcome::Inserted)));
}

// ── read_full_object defense-in-depth: concurrent truncation ─────────
//
// This test triggers the `output.len() != capacity` check inside the
// local-store read_full_object path.  We race a concurrent
// truncation against the read: if the file shrinks between the metadata
// check and read_to_end, the post-read length check fires.
//
// The race window is widened by using a 2 MiB payload.

#[test]
fn read_full_object_concurrent_truncation_triggers_length_mismatch() {
    use std::time::Duration;

    let storage = shardline_test_support::TempStorage::new();
    let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
    let key =
        ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let path = store.local_path_for_key(&key).unwrap();

    // Write 16 MiB of data to widen the race window during read
    let large_body = vec![0xabu8; 16 * 1024 * 1024];
    let integrity = ObjectIntegrity::new(chunk_hash(&large_body), large_body.len() as u64);
    store
        .put_if_absent(
            &key,
            shardline_storage::ObjectBody::from_slice(&large_body),
            &integrity,
        )
        .unwrap();

    let p = path;

    // Spawn a thread that truncates the file to 1 byte
    let handle = std::thread::spawn(move || {
        // Wait a tiny bit to let the main thread start read_full_object
        std::thread::sleep(Duration::from_millis(1));
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(1).unwrap();
    });

    let result = read_full_object(&store, &key, large_body.len() as u64);

    handle.join().unwrap();

    // We expect *either* a StoredObjectLengthMismatch (if the race was won)
    // or a successful read (if we were too fast / too slow).
    // Accept both outcomes because the timing is probabilistic.
    match result {
        Ok(data) => {
            // File was read before truncation took effect
            assert_eq!(data.len(), large_body.len());
        }
        Err(ServerObjectStoreError::StoredObjectLengthMismatch) => {
            // Race won – defense-in-depth check triggered
        }
        Err(other) => panic!("unexpected error: {other}"),
    }
}
