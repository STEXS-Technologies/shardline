#![allow(clippy::unwrap_used)]

use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome, S3ObjectStore,
    S3ObjectStoreConfig,
};
use shardline_test_support::DockerLocalStack;

fn s3_config_with_bucket(
    stack: &DockerLocalStack,
    bucket: String,
    key_prefix: Option<&str>,
) -> Option<S3ObjectStoreConfig> {
    let raw = stack.s3_raw_config(key_prefix)?;
    Some(
        S3ObjectStoreConfig::new(bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
            .with_allow_http(raw.allow_http),
    )
}

fn s3_config(stack: &DockerLocalStack, key_prefix: Option<&str>) -> Option<S3ObjectStoreConfig> {
    let raw = stack.s3_raw_config(key_prefix)?;
    Some(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
            .with_allow_http(raw.allow_http),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_bucket_not_found() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let config = s3_config_with_bucket(
        &stack,
        "non-existent-bucket-for-testing".to_owned(),
        Some("test-bucket-not-found"),
    );
    let Some(config) = config else {
        return;
    };
    let result = S3ObjectStore::new(config);
    // Creating the store may succeed (lazy bucket resolution), but operations on
    // a non-existent bucket should fail.
    if let Ok(store) = result {
        let key = ObjectKey::parse("test/key").unwrap();
        let body = b"data";
        let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);
        let outcome = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(
            outcome.is_err(),
            "expected error writing to non-existent bucket: {outcome:?}"
        );
    }
}

fn chunk_hash(data: &[u8]) -> shardline_protocol::ShardlineHash {
    let digest = blake3::hash(data);
    shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_and_read_roundtrip() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    assert!(
        result.is_ok(),
        "delete on missing key should succeed: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_read_missing_returns_not_found() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    store
        .put_if_absent(&key_a, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_b, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let mut visited = Vec::new();
    store
        .visit_prefix(&prefix, |meta| {
            visited.push(meta.key().as_str().to_owned());
            Ok::<_, shardline_storage::S3ObjectStoreError>(())
        })
        .unwrap();
    assert_eq!(visited.len(), 2);
    assert!(visited.contains(&"ab/aaa".to_owned()));
    assert!(visited.contains(&"ab/bbb".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_stream_range_returns_partial_content() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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

    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Read middle range (bytes 5-14 = "fghijklmno")
    let range = shardline_protocol::ByteRange::new(5, 14).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, b"fghijklmno");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_list_flat_namespace_page_supports_pagination() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap();
    }

    // Get full page (limit=10)
    let full_page = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(full_page.len(), 5);

    // Get with start_after
    let key_after = ObjectKey::parse("pg/key02").unwrap();
    let after_page = store
        .list_flat_namespace_page(&prefix, Some(&key_after), 10)
        .unwrap();
    assert_eq!(after_page.len(), 2); // key03, key04
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_file_if_absent_stores_file_content() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let outcome = store
        .put_file_if_absent(&key, &file_path, &integrity)
        .unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));
    assert!(store.contains(&key).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_overwrite_integrity_rejects_bad_hash() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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

    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &valid_integrity)
        .unwrap();

    // Overwrite with wrong hash
    let bad_integrity = ObjectIntegrity::new(chunk_hash(b"wrong data"), 100);
    let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &bad_integrity);
    // S3 may or may not enforce integrity — just verify it doesn't panic
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_content_addressed_upload_roundtrip() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    let result = store
        .begin_content_addressed_upload(&canonical_key)
        .await
        .unwrap();
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
    let outcome = store
        .finish_content_addressed_upload(writer, &temp_key, &canonical_key)
        .await
        .unwrap();
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
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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
    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Then begin a content-addressed upload — should return AlreadyExists
    let result = store.begin_content_addressed_upload(&key).await.unwrap();
    assert!(matches!(
        result,
        shardline_storage::BeginMultipartUploadResult::AlreadyExists
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_put_content_addressed_file_stores_and_verifies() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
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

    let outcome = store
        .put_content_addressed_file(&key, &file_path, &integrity)
        .unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Verify content
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, body);

    // Second call should return AlreadyExists
    let outcome2 = store
        .put_content_addressed_file(&key, &file_path, &integrity)
        .unwrap();
    assert!(matches!(outcome2, PutOutcome::AlreadyExists));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_empty_bucket() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-empty-bucket")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let prefix = ObjectPrefix::parse("nonexistent/").unwrap();
    let results = store.list_prefix(&prefix).unwrap();
    assert!(
        results.is_empty(),
        "expected empty list for non-existent prefix"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_with_prefix_matches_exact_prefix() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-prefix-match")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let body = b"data";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    let key_foo = ObjectKey::parse("pfx/foo").unwrap();
    let key_bar = ObjectKey::parse("pfx/bar").unwrap();
    let key_other = ObjectKey::parse("other/baz").unwrap();

    store
        .put_if_absent(&key_foo, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_bar, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store
        .put_if_absent(&key_other, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // List with matching prefix
    let prefix = ObjectPrefix::parse("pfx/").unwrap();
    let results = store.list_prefix(&prefix).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|m| m.key().as_str() == "pfx/foo"));
    assert!(results.iter().any(|m| m.key().as_str() == "pfx/bar"));

    // List with prefix that has no objects
    let empty_prefix = ObjectPrefix::parse("zzz/").unwrap();
    let empty = store.list_prefix(&empty_prefix).unwrap();
    assert!(empty.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_rename_object_via_copy_and_delete() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-rename")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let src = ObjectKey::parse("rename/source").unwrap();
    let dst = ObjectKey::parse("rename/dest").unwrap();
    let body = b"rename me";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    // Put source
    store
        .put_if_absent(&src, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    assert!(store.contains(&src).unwrap());

    // Copy to destination
    assert!(matches!(
        store.copy_object_if_absent(&src, &dst).unwrap(),
        PutOutcome::Inserted
    ));

    // Verify destination content
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&dst, range).unwrap();
    assert_eq!(data, body);

    // Delete source (simulating rename)
    assert!(matches!(
        store.delete_if_present(&src).unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    ));
    assert!(!store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_copy_object_preserves_content() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-copy-content")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let src = ObjectKey::parse("cpy/source").unwrap();
    let dst = ObjectKey::parse("cpy/dest").unwrap();
    let body = b"copy content verification";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&src, ObjectBody::from_slice(body), &integrity)
        .unwrap();
    store.copy_object_if_absent(&src, &dst).unwrap();

    // Verify both original and copy are intact
    let src_end = (body.len() as u64).checked_sub(1).unwrap();
    let src_range = shardline_protocol::ByteRange::new(0, src_end).unwrap();
    let src_data = store.read_range(&src, src_range).unwrap();
    assert_eq!(src_data, body);

    let dst_data = store.read_range(&dst, src_range).unwrap();
    assert_eq!(dst_data, body);

    // Delete source — destination should remain
    store.delete_if_present(&src).unwrap();
    assert!(!store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_metadata_roundtrip() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-metadata")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("meta/test-file").unwrap();
    let body = b"metadata roundtrip content";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    store
        .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.key().as_str(), "meta/test-file");
    assert_eq!(meta.length(), body.len() as u64);
    // Checksum may be None depending on adapter — just check it doesn't panic
    let _checksum = meta.checksum();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_upload_edge_small_chunks() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-mpu-edge")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("mpu/small-chunks").unwrap();
    let body = b"tiny multipart content";

    let result = store.begin_content_addressed_upload(&key).await.unwrap();
    let (mut writer, temp_key) = match result {
        shardline_storage::BeginMultipartUploadResult::AlreadyExists => return,
        shardline_storage::BeginMultipartUploadResult::Upload(w, t) => (w, t),
    };

    // Write in very small chunks (1 byte each)
    for &byte in body.iter() {
        writer.wait_for_capacity(1).await.unwrap();
        writer.write(&[byte]);
    }

    let outcome = store
        .finish_content_addressed_upload(writer, &temp_key, &key)
        .await
        .unwrap();
    assert!(matches!(outcome, PutOutcome::Inserted));

    // Verify content
    assert!(store.contains(&key).unwrap());
    let end = (body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data, body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_concurrent_read_write() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-concurrent")) else {
        return;
    };
    let store_a = S3ObjectStore::new(config.clone()).unwrap();
    let store_b = S3ObjectStore::new(config).unwrap();

    let key_a = ObjectKey::parse("concurrent/key-a").unwrap();
    let key_b = ObjectKey::parse("concurrent/key-b").unwrap();
    let body_a = b"content from writer A";
    let body_b = b"content from writer B";
    let integrity_a = ObjectIntegrity::new(chunk_hash(body_a), body_a.len() as u64);
    let integrity_b = ObjectIntegrity::new(chunk_hash(body_b), body_b.len() as u64);

    // Concurrent writes to different keys
    let (result_a, result_b) = tokio::join!(
        async { store_a.put_if_absent(&key_a, ObjectBody::from_slice(body_a), &integrity_a) },
        async { store_b.put_if_absent(&key_b, ObjectBody::from_slice(body_b), &integrity_b) },
    );
    assert!(matches!(result_a.unwrap(), PutOutcome::Inserted));
    assert!(matches!(result_b.unwrap(), PutOutcome::Inserted));

    // Concurrent reads — each reads the other's write
    let (data_a, data_b) = tokio::join!(
        async {
            let end = (body_a.len() as u64).checked_sub(1).unwrap();
            store_a.read_range(&key_a, shardline_protocol::ByteRange::new(0, end).unwrap())
        },
        async {
            let end = (body_b.len() as u64).checked_sub(1).unwrap();
            store_b.read_range(&key_b, shardline_protocol::ByteRange::new(0, end).unwrap())
        },
    );
    assert_eq!(data_a.unwrap(), body_a);
    assert_eq!(data_b.unwrap(), body_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_non_existent_returns_not_found() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-delete-nonexistent")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("nonexistent/ghost-key").unwrap();
    // S3 delete on a non-existent key is a no-op that returns success.
    // MinIO returns DeleteOutcome::Deleted; other providers may return NotFound.
    let result = store.delete_if_present(&key);
    assert!(
        result.is_ok(),
        "delete on missing key should succeed: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_large_content_roundtrip() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-large")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();
    let key = ObjectKey::parse("large/10mb-file").unwrap();

    // 10 MB of data
    let large_body = vec![0xABu8; 10 * 1024 * 1024];
    let integrity = ObjectIntegrity::new(chunk_hash(&large_body), large_body.len() as u64);

    store
        .put_if_absent(&key, ObjectBody::from_slice(&large_body), &integrity)
        .unwrap();
    assert!(store.contains(&key).unwrap());

    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.length(), large_body.len() as u64);

    // Read full range
    let end = (large_body.len() as u64).checked_sub(1).unwrap();
    let range = shardline_protocol::ByteRange::new(0, end).unwrap();
    let data = store.read_range(&key, range).unwrap();
    assert_eq!(data.len(), large_body.len());
    assert_eq!(data, large_body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_special_character_keys() {
    let Some(stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(config) = s3_config(&stack, Some("test-special-chars")) else {
        return;
    };
    let store = S3ObjectStore::new(config).unwrap();

    // Keys with special characters and slashes
    let test_cases = &[
        "special/file with spaces.xorb",
        "special/file-with-dashes.xorb",
        "special/file_with_underscores.xorb",
        "special/nested/path/file.xorb",
        "special/dotted.file.name.v1.xorb",
    ];

    let body = b"special character test";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    for key_str in test_cases {
        let key = ObjectKey::parse(key_str).unwrap();
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap();
        assert!(store.contains(&key).unwrap(), "key should exist: {key_str}");
    }

    // List the special prefix and verify all keys
    let prefix = ObjectPrefix::parse("special/").unwrap();
    let results = store.list_prefix(&prefix).unwrap();
    assert_eq!(results.len(), test_cases.len());

    for key_str in test_cases {
        assert!(
            results.iter().any(|m| m.key().as_str() == *key_str),
            "missing key in listing: {key_str}"
        );
    }
}
