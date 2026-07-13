#![allow(clippy::unwrap_used)]

use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome, S3ObjectStore,
    S3ObjectStoreConfig,
};
use shardline_test_support::DockerLocalStack;

fn s3_config(stack: &DockerLocalStack, key_prefix: Option<&str>) -> Option<S3ObjectStoreConfig> {
    let raw = stack.s3_raw_config(key_prefix)?;
    Some(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
            .with_allow_http(raw.allow_http),
    )
}

fn chunk_hash(data: &[u8]) -> shardline_protocol::ShardlineHash {
    let digest = blake3::hash(data);
    shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_and_read_roundtrip() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-put-read")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/hello.txt").unwrap();
    let body = b"Hello, MinIO!";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    // Put
    let outcome = store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Contains
    assert!(store.contains(&key).unwrap());

    // Metadata
    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.length(), body.len() as u64);

    // Read full range
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, body);

    // Delete
    assert!(matches!(
        store.delete_if_present(&key).unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    ));
    assert!(!store.contains(&key).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_is_idempotent() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-idempotent")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/idempotent.txt").unwrap();
    let body = b"idempotent";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    assert!(matches!(
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap(),
        PutOutcome::Inserted
    ));
    assert!(matches!(
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap(),
        PutOutcome::AlreadyExists
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_overwrite_replaces_content() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-overwrite")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/overwrite.txt").unwrap();

    let original = b"original content";
    let replacement = b"replacement data here";
    store
        .put_overwrite(
            &key,
            ObjectBody::from_slice(original),
            &ObjectIntegrity::new(chunk_hash(original), original.len() as u64),
        )
        .unwrap();
    store
        .put_overwrite(
            &key,
            ObjectBody::from_slice(replacement),
            &ObjectIntegrity::new(chunk_hash(replacement), replacement.len() as u64),
        )
        .unwrap();

    let end = (replacement.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, replacement);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_prefix_filters_by_prefix() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-list-prefix")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let key_a = ObjectKey::parse("ab/aaa").unwrap();
    let key_b = ObjectKey::parse("ab/bbb").unwrap();
    let key_c = ObjectKey::parse("ac/ccc").unwrap();

    store
        .put_if_absent(&key_a, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_b, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_c, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let prefix = ObjectPrefix::parse("ab/").unwrap();
    let results = store.list_prefix(&prefix).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|m| m.key().as_str() == "ab/aaa"));
    assert!(results.iter().any(|m| m.key().as_str() == "ab/bbb"));
    assert!(!results.iter().any(|m| m.key().as_str() == "ac/ccc"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_delete_missing_returns_not_found() {
        let Some(stack) = DockerLocalStack::builder()
            .with_minio()
            .start()
            .unwrap()
        else {
            eprintln!("skipping: docker not available");
            return;
        };
        let Some(config) = s3_config(&stack, Some("test-delete-missing")) else {
            return;
        };
        let store = S3ObjectStore::new(config).unwrap();
        let key = ObjectKey::parse("ab/nonexistent").unwrap();
        // S3 delete on a non-existent key returns success (no error).
        // Different S3 adapters may return NotFound or Deleted.
        let result = store.delete_if_present(&key);
        assert!(result.is_ok(), "delete on missing key should succeed: {result:?}");
    }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_read_missing_returns_not_found() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-read-missing")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/missing").unwrap();
    let range = shardline_protocol::ByteRange::new(0, 4).unwrap();
    let result = store.read_range(&key, range);
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_copy_object_if_absent() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-copy")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();

    let src = ObjectKey::parse("ab/source").unwrap();
    let dst = ObjectKey::parse("cd/dest").unwrap();
    let body = b"copy me";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&src, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    assert!(matches!(
        store.copy_object_if_absent(&src, &dst).unwrap(),
        PutOutcome::Inserted
    ));
    assert!(matches!(
        store.copy_object_if_absent(&src, &dst).unwrap(),
        PutOutcome::AlreadyExists
    ));

    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&dst, range).unwrap();
    assert_eq!(data, body);
}


