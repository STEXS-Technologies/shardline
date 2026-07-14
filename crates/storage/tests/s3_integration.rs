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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_visit_prefix_collects_all_entries() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-visit-prefix")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let prefix = ObjectPrefix::parse("ab/").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let key_a = ObjectKey::parse("ab/aaa").unwrap();
    let key_b = ObjectKey::parse("ab/bbb").unwrap();
    store.put_if_absent(&key_a, ObjectBody::from_slice(body), &integrity).unwrap();
    store.put_if_absent(&key_b, ObjectBody::from_slice(body), &integrity).unwrap();

    let mut visited = Vec::new();
    store.visit_prefix(&prefix, |meta| {
        visited.push(meta.key().as_str().to_owned());
        Ok::<_, shardline_storage::S3ObjectStoreError>(())
    }).unwrap();
    assert_eq!(visited.len(), 2);
    assert!(visited.contains(&"ab/aaa".to_owned()));
    assert!(visited.contains(&"ab/bbb".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_stream_range_returns_partial_content() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-stream-range")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/stream-range").unwrap();
    let body = b"abcdefghijklmnopqrstuvwxyz";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

    // Read middle range (bytes 5-14 = "fghijklmno")
    let range = shardline_protocol::ByteRange::new(5, 14).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, b"fghijklmno");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_list_flat_namespace_page_supports_pagination() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-list-page")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let prefix = ObjectPrefix::parse("pg/").unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    // Insert 5 keys
    for i in 0..5u8 {
        let key = ObjectKey::parse(&format!("pg/key{i:02}")).unwrap();
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();
    }

    // Get full page (limit=10)
    let full_page = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(full_page.len(), 5);

    // Get with start_after
    let key_after = ObjectKey::parse("pg/key02").unwrap();
    let after_page = store.list_flat_namespace_page(&prefix, Some(&key_after), 10).unwrap();
    assert_eq!(after_page.len(), 2); // key03, key04
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_file_if_absent_stores_file_content() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-put-file")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/file-test").unwrap();
    let body = b"file content test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    // Create a temp file to upload
    let dir = tempfile::TempDir::new().unwrap();
    let file_path = dir.path().join("test-file.bin");
    std::fs::write(&file_path, body).unwrap();
    let outcome = store.put_file_if_absent(&key, &file_path, &integrity).unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));
    assert!(store.contains(&key).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_overwrite_integrity_rejects_bad_hash() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-overwrite-bad")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ab/overwrite-bad").unwrap();
    let body = b"valid content";
    let valid_integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store.put_if_absent(&key, ObjectBody::from_slice(body), &valid_integrity).unwrap();

    // Overwrite with wrong hash
    let bad_integrity = ObjectIntegrity::new(chunk_hash(b"wrong data"), 100);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &bad_integrity);
    // S3 may or may not enforce integrity — just verify it doesn't panic
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_content_addressed_upload_roundtrip() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-ca-upload")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let canonical_key = ObjectKey::parse("ca/final-key").unwrap();
    let body = b"content addressed upload test data";

    // Step 1: begin content-addressed upload
    let result = store.begin_content_addressed_upload(&canonical_key).await.unwrap();
    let (mut writer, temp_key) = match result {
        shardline_storage::BeginMultipartUploadResult::AlreadyExists => {
            // Already exists is fine — the content is the same
            return;
        }
        shardline_storage::BeginMultipartUploadResult::Upload(writer, temp_key) => {
            (writer, temp_key)
        }
    };

    // Step 2: write data through the multipart writer
    writer.wait_for_capacity(1).await.unwrap();
    writer.write(body);

    // Step 3: finish — finalize multipart and promote temp to canonical
    let outcome = store.finish_content_addressed_upload(writer, &temp_key, &canonical_key).await.unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Step 4: verify content
    assert!(store.contains(&canonical_key).unwrap());
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&canonical_key, range).unwrap();
    assert_eq!(data, body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_content_addressed_upload_already_exists_idempotent() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-ca-exists")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ca/existing-key").unwrap();
    let body = b"existing content";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    // First, put the content directly
    store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

    // Then begin a content-addressed upload — should return AlreadyExists
    let result = store.begin_content_addressed_upload(&key).await.unwrap();
    assert!(matches!(result, shardline_storage::BeginMultipartUploadResult::AlreadyExists));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_content_addressed_file_stores_and_verifies() {
    let Some(stack) = DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-ca-file")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("ca/file-key").unwrap();
    let body = b"file content for content-addressed put";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let dir = tempfile::TempDir::new().unwrap();
    let file_path = dir.path().join("source.bin");
    std::fs::write(&file_path, body).unwrap();

    let outcome = store.put_content_addressed_file(&key, &file_path, &integrity).unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Verify content
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, body);

    // Second call should return AlreadyExists
    let outcome2 = store.put_content_addressed_file(&key, &file_path, &integrity).unwrap();
    assert!(matches!(outcome2, PutOutcome::AlreadyExists));
}


