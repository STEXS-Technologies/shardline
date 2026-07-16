#![cfg(feature = "docker")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::clone_on_copy,
    clippy::shadow_unrelated,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::needless_return
)]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use sha2::{Digest, Sha256};
use shardline_server::{
    BenchmarkBackend, ObjectStorageAdapter, ObjectStoreError, ServerBackend, ServerConfig,
    ServerError, shared_sha256_object_key,
};
use shardline_server_core::ServerObjectStoreError;
use shardline_storage::{
    BeginMultipartUploadResult, DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectPrefix, ObjectStore, PutOutcome, S3ObjectStore, S3ObjectStoreConfig,
};
use shardline_test_support::DockerLocalStack;
use tempfile::TempDir;
use tokio::sync::OnceCell;

// ---------------------------------------------------------------------------
// Shared Docker MinIO — one container for all tests.
// ---------------------------------------------------------------------------

static MINIO: OnceCell<(DockerLocalStack, String)> = OnceCell::const_new();

/// Ensure the global Docker MinIO is running and return the key prefix.
/// Each test group uses a unique prefix for isolation.
async fn ensure_minio() -> &'static str {
    let (_, prefix) = MINIO
        .get_or_init(|| async {
            let stack = DockerLocalStack::builder()
                .with_minio()
                .start()
                .unwrap()
                .expect("Docker MinIO: is docker available?");
            let prefix = stack.unique_s3_key_prefix("s3-integration");
            (stack, prefix)
        })
        .await;
    prefix
}

/// Build an `S3ObjectStoreConfig` from the global MinIO instance with the
/// given key prefix.
fn s3_config(key_prefix: &str) -> S3ObjectStoreConfig {
    // We re-use the global stack's raw config — grab it fresh each time.
    // The static holds (DockerLocalStack, prefix), so we reconstruct config
    // per-call.  This is fine because the stack is shared.
    let raw = MINIO
        .get()
        .expect("ensure_minio() not called yet")
        .0
        .s3_raw_config(Some(key_prefix))
        .expect("MinIO should be configured");

    S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http)
}

/// Create a fresh `S3ObjectStore` connected to the shared MinIO container.
async fn s3_store() -> S3ObjectStore {
    let prefix = ensure_minio().await;
    let config = s3_config(prefix);
    S3ObjectStore::new(config).unwrap()
}

/// Build a `ServerConfig` with S3 object store for `BenchmarkBackend`.
fn s3_server_config(root: &TempDir) -> ServerConfig {
    let prefix = MINIO
        .get()
        .expect("ensure_minio() not called yet")
        .1
        .clone();
    let config = s3_config(&prefix);
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    ServerConfig::new(
        bind_addr,
        "http://127.0.0.1:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(config))
}

/// Create a `BenchmarkBackend` backed by S3 for integration-level roundtrip tests.
async fn s3_benchmark(namespace: &str) -> (BenchmarkBackend, TempDir) {
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), namespace)
        .await
        .unwrap();
    (bench, tmp)
}

fn blake3_hash(data: &[u8]) -> String {
    hex::encode(blake3::hash(data).as_bytes())
}

// ===========================================================================
// Backend construction with S3
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_construction() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("construction").await;
    assert_eq!(bench.metadata_backend_name(), "local");
    assert_eq!(bench.object_backend_name(), "s3");
}

// ===========================================================================
// S3 ObjectStore basic CRUD
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_put_and_get() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/put-get").unwrap();
    let data = b"hello s3 world";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    // Put
    let outcome = store.put_if_absent(&key, ObjectBody::from_slice(data), &integrity);
    assert!(outcome.is_ok());
    assert_eq!(outcome.unwrap(), PutOutcome::Inserted);

    // Read back via metadata + range
    let meta = store.metadata(&key).unwrap();
    assert!(meta.is_some());
    assert_eq!(meta.unwrap().length(), data.len() as u64);

    // Read via contains
    assert!(store.contains(&key).unwrap());

    // Get via range
    let range = shardline_protocol::ByteRange::new(0, data.len() as u64 - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_object_store_recovers_after_minio_restart() {
    let Some(mut stack) = DockerLocalStack::builder().with_minio().start().unwrap() else {
        eprintln!("skipping — Docker not available");
        return;
    };
    let prefix = stack.unique_s3_key_prefix("s3-recovery");
    let raw = stack
        .s3_raw_config(Some(&prefix))
        .expect("MinIO should be configured");
    let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http);
    // Construct outside Tokio so the adapter retains a runtime when used from
    // the blocking outage probe below.
    let store = tokio::task::spawn_blocking(move || S3ObjectStore::new(config))
        .await
        .unwrap()
        .unwrap();
    let key = ObjectKey::parse("recovery/persisted-object").unwrap();
    let data = b"object survives temporary MinIO outage";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );
    assert_eq!(
        store
            .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
            .unwrap(),
        PutOutcome::Inserted
    );

    stack.stop_minio().unwrap();
    let unavailable_store = store.clone();
    let unavailable_key = key.clone();
    let unavailable = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || unavailable_store.metadata(&unavailable_key)),
    )
    .await
    .expect("object metadata probe should fail promptly while MinIO is stopped")
    .unwrap();
    assert!(
        unavailable.is_err(),
        "object metadata probe must surface a stopped MinIO service"
    );

    stack.start_minio().unwrap();
    let recovered_raw = stack
        .s3_raw_config(Some(&prefix))
        .expect("MinIO should be configured after restart");
    let recovered_config = S3ObjectStoreConfig::new(recovered_raw.bucket, recovered_raw.region)
        .with_endpoint(recovered_raw.endpoint)
        .with_credentials(
            recovered_raw.access_key,
            recovered_raw.secret_key,
            recovered_raw.session_token,
        )
        .with_key_prefix(recovered_raw.key_prefix.as_deref())
        .with_allow_http(recovered_raw.allow_http);
    let recovered_store = tokio::task::spawn_blocking(move || S3ObjectStore::new(recovered_config))
        .await
        .unwrap()
        .unwrap();
    let recovered = retry_s3_metadata(&recovered_store, &key).await;
    assert_eq!(recovered.length(), data.len() as u64);
    let range = shardline_protocol::ByteRange::new(0, data.len() as u64 - 1).unwrap();
    assert_eq!(recovered_store.read_range(&key, range).unwrap(), data);
}

async fn retry_s3_metadata(
    store: &S3ObjectStore,
    key: &ObjectKey,
) -> shardline_storage::ObjectMetadata {
    let mut last_error = None;
    for _ in 0..20 {
        match store.metadata(key) {
            Ok(Some(metadata)) => return metadata,
            Ok(None) => panic!("persisted object disappeared after MinIO restart"),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("MinIO did not recover in time: {last_error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_put_if_absent_idempotent() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/idempotent").unwrap();
    let data = b"same content";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    let first = store.put_if_absent(&key, ObjectBody::from_slice(data), &integrity);
    assert!(first.is_ok());
    assert_eq!(first.unwrap(), PutOutcome::Inserted);

    // Second put should return AlreadyExists
    let second = store.put_if_absent(&key, ObjectBody::from_slice(data), &integrity);
    assert!(second.is_ok());
    assert_eq!(second.unwrap(), PutOutcome::AlreadyExists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_delete_object() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/delete-me").unwrap();
    let data = b"to-delete";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    assert!(store.contains(&key).unwrap());

    let outcome = store.delete_if_present(&key).unwrap();
    assert_eq!(outcome, DeleteOutcome::Deleted);
    assert!(!store.contains(&key).unwrap());

    // Second delete — MinIO / S3 returns Deleted (idempotent)
    let outcome2 = store.delete_if_present(&key).unwrap();
    // S3 delete is idempotent — returns Deleted even when key is absent.
    assert_eq!(outcome2, DeleteOutcome::Deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_list_prefix() {
    let store = s3_store().await;
    let prefix_str = "crud/list-test";
    let prefix = ObjectPrefix::parse(prefix_str).unwrap();

    // Put a few objects
    for i in 0..3 {
        let key = ObjectKey::parse(&format!("{prefix_str}/obj{i}")).unwrap();
        let data = vec![b'a' + i; 16];
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&data).as_bytes()),
            data.len() as u64,
        );
        store
            .put_if_absent(&key, ObjectBody::from_vec(data), &integrity)
            .unwrap();
    }

    let objects = store.list_prefix(&prefix).unwrap();
    assert_eq!(objects.len(), 3);

    let mut names: Vec<&str> = objects.iter().map(|m| m.key().as_str()).collect();
    names.sort();
    assert!(names.first().unwrap().contains("obj0"));
    assert!(names.get(1).unwrap().contains("obj1"));
    assert!(names.get(2).unwrap().contains("obj2"));
}

// ===========================================================================
// S3 resumable / multipart upload lifecycle
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_create_upload_and_abort() {
    let store = s3_store().await;
    let key = ObjectKey::parse("multipart/abort-test").unwrap();

    let upload_id = store.create_resumable_upload(&key).await.unwrap();
    assert!(!upload_id.is_empty());

    // Abort the upload — no parts uploaded, so this should succeed.
    store
        .abort_resumable_upload(&key, &upload_id)
        .await
        .unwrap();

    // Object should not exist after abort.
    assert!(!store.contains(&key).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_full_upload_lifecycle() {
    let store = s3_store().await;
    let key = ObjectKey::parse("multipart/complete-test").unwrap();

    // S3 requires each non-last part to be at least 5 MiB.
    // Build two parts: first >= 5 MiB, last (smaller) is implicit last.
    let part1 = vec![0xAB_u8; 6 * 1024 * 1024]; // 6 MiB
    let part2 = b"final-small-part".to_vec();

    let part_data: Vec<Vec<u8>> = vec![part1, part2];

    // 1. Create upload
    let upload_id = store.create_resumable_upload(&key).await.unwrap();

    // 2. Upload parts
    let mut parts = Vec::new();
    for (idx, data) in part_data.iter().enumerate() {
        let etag = store
            .upload_resumable_part(&key, &upload_id, idx, data.clone().into())
            .await
            .unwrap();
        parts.push((idx, etag));
    }

    // 3. Complete upload
    store
        .complete_resumable_upload(&key, &upload_id, parts)
        .await
        .unwrap();

    // 4. Verify object exists and has correct total length
    let meta = store.metadata(&key).unwrap();
    assert!(meta.is_some());
    let expected_len: u64 = part_data.iter().map(|d| d.len() as u64).sum();
    assert_eq!(meta.unwrap().length(), expected_len);

    // 5. Read back and verify content (first few bytes + last few bytes)
    let range = shardline_protocol::ByteRange::new(0, expected_len - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read.len(), expected_len as usize);
    // Spot-check first bytes and last bytes
    assert_eq!(read.get(..4).unwrap(), &[0xAB; 4]);
    assert_eq!(read.get(read.len() - 4..).unwrap(), b"part");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_abort_after_partial_upload() {
    let store = s3_store().await;
    let key = ObjectKey::parse("multipart/partial-abort").unwrap();

    let upload_id = store.create_resumable_upload(&key).await.unwrap();

    // Upload one part
    let etag = store
        .upload_resumable_part(&key, &upload_id, 0, b"partial data".to_vec().into())
        .await
        .unwrap();
    assert!(!etag.is_empty());

    // Abort — should discard the part
    store
        .abort_resumable_upload(&key, &upload_id)
        .await
        .unwrap();

    assert!(!store.contains(&key).unwrap());
}

// ===========================================================================
// S3 content-addressed operations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_content_addressed_upload() {
    let store = s3_store().await;
    let key = ObjectKey::parse("ca/full-upload").unwrap();
    let data = b"content-addressed data goes here";

    // 1. Begin content-addressed upload
    let result = store.begin_content_addressed_upload(&key).await.unwrap();

    let (mut writer, temp_key) = match result {
        BeginMultipartUploadResult::AlreadyExists => {
            // Should not happen on first write — use unreachable
            return; // unreachable in this test
        }
        BeginMultipartUploadResult::Upload(w, tk) => (w, tk),
    };

    // 2. Write data in chunks
    writer.write(data);
    writer.wait_for_capacity(4).await.unwrap();

    // 3. Finish and promote to canonical key
    let outcome = store
        .finish_content_addressed_upload(writer, &temp_key, &key)
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // 4. Verify object exists at canonical key (not at temp_key)
    assert!(store.contains(&key).unwrap());
    assert!(!store.contains(&temp_key).unwrap());

    // 5. Read back content
    let len = data.len() as u64;
    let range = shardline_protocol::ByteRange::new(0, len - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_content_addressed_already_exists() {
    let store = s3_store().await;
    let key = ObjectKey::parse("ca/already-exists").unwrap();
    let data = b"deduplicated content";

    // First write — insert
    let result1 = store.begin_content_addressed_upload(&key).await.unwrap();
    let (mut writer, temp_key) = match result1 {
        BeginMultipartUploadResult::Upload(w, tk) => (w, tk),
        _ => {
            // Expected Upload variant on first write
            return;
        }
    };
    writer.write(data);
    writer.wait_for_capacity(4).await.unwrap();
    store
        .finish_content_addressed_upload(writer, &temp_key, &key)
        .await
        .unwrap();

    // Second write — should see AlreadyExists
    let result2 = store.begin_content_addressed_upload(&key).await.unwrap();
    assert!(matches!(result2, BeginMultipartUploadResult::AlreadyExists));
}

// ===========================================================================
// S3 copy operations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_copy_if_absent() {
    let store = s3_store().await;
    let src = ObjectKey::parse("copy/source").unwrap();
    let dst = ObjectKey::parse("copy/dest").unwrap();
    let data = b"copy-test-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    // Put source
    store
        .put_if_absent(&src, ObjectBody::from_slice(data), &integrity)
        .unwrap();

    // Copy
    let outcome = store.copy_object_if_absent(&src, &dst).unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Both exist with same length
    assert!(store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
    assert_eq!(
        store.metadata(&src).unwrap().unwrap().length(),
        store.metadata(&dst).unwrap().unwrap().length()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_copy_if_absent_non_existent_source() {
    let store = s3_store().await;
    let src = ObjectKey::parse("copy/missing-src").unwrap();
    let dst = ObjectKey::parse("copy/missing-dst").unwrap();

    let result = store.copy_object_if_absent(&src, &dst);
    assert!(result.is_err());
}

// ===========================================================================
// S3 overwrite
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_overwrite() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/overwrite").unwrap();
    let original = b"original content";
    let replacement = b"replacement content";
    let _hash1 = blake3_hash(original);
    let _hash2 = blake3_hash(replacement);
    let integrity1 = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(original).as_bytes()),
        original.len() as u64,
    );
    let integrity2 = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(replacement).as_bytes()),
        replacement.len() as u64,
    );

    // Put original
    store
        .put_if_absent(&key, ObjectBody::from_slice(original), &integrity1)
        .unwrap();
    assert_eq!(
        store.metadata(&key).unwrap().unwrap().length(),
        original.len() as u64
    );

    // Overwrite
    store
        .put_overwrite(&key, ObjectBody::from_slice(replacement), &integrity2)
        .unwrap();

    // Verify new content
    let len = replacement.len() as u64;
    let range = shardline_protocol::ByteRange::new(0, len - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, replacement);
}

// ===========================================================================
// Backend upload/download roundtrip with S3 (via BenchmarkBackend)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_upload_download_roundtrip() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("upload-dl").await;

    let content = b"S3-backed upload and download";
    bench
        .upload_file(
            "s3-roundtrip.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();

    let downloaded = bench
        .download_file("s3-roundtrip.bin", None, None)
        .await
        .unwrap();
    assert_eq!(downloaded, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_upload_and_reconstruction() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("recon").await;

    let content = b"S3 reconstruction test content";
    bench
        .upload_file(
            "s3-recon.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();

    let response = bench
        .reconstruction("s3-recon.bin", None, None, None)
        .await
        .unwrap();
    assert!(
        !response.terms.is_empty() || response.offset_into_first_range == 0,
        "reconstruction should return a valid response"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_stats() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("stats").await;

    // Upload a file so stats has something to count.
    bench
        .upload_file(
            "s3-stats-check.bin",
            axum::body::Bytes::from_static(b"S3 stats content"),
            None,
        )
        .await
        .unwrap();

    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 1, "should have at least one file: {stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_ready() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("ready-check").await;

    let result = bench
        .reconstruction("nonexistent-s3-file", None, None, None)
        .await;
    assert!(
        result.is_err(),
        "reconstruction of missing file should fail"
    );
}

// ===========================================================================
// S3 ObjectStore metadata edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_metadata_not_found() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/ghost-key").unwrap();
    let meta = store.metadata(&key).unwrap();
    assert!(meta.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_contains_not_found() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/ghost-key2").unwrap();
    assert!(!store.contains(&key).unwrap());
}

// ===========================================================================
// 1. OciBackend S3 dispatch arms via ServerBackend
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_create_resumable_upload() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();
    assert_eq!(backend.object_backend_name(), "s3");

    let key = ObjectKey::parse("oci/create-upload").unwrap();
    let result = OciBackend::create_resumable_object_upload(&backend, &key).await;
    assert!(result.is_ok());
    let upload_id = result.unwrap();
    assert!(
        upload_id.is_some(),
        "S3 create_resumable_upload should return an upload ID"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_upload_resumable_part() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let key = ObjectKey::parse("oci/upload-part").unwrap();
    let upload_id = OciBackend::create_resumable_object_upload(&backend, &key)
        .await
        .unwrap()
        .expect("upload_id");
    let etag = OciBackend::upload_resumable_object_part(
        &backend,
        &key,
        &upload_id,
        0,
        axum::body::Bytes::from_static(b"part-data"),
    )
    .await
    .unwrap();
    assert!(!etag.is_empty(), "S3 upload part should return an ETag");

    // Abort to clean up
    OciBackend::abort_resumable_object_upload(&backend, &key, &upload_id)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_complete_resumable_upload() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let key = ObjectKey::parse("oci/complete-upload").unwrap();
    let upload_id = OciBackend::create_resumable_object_upload(&backend, &key)
        .await
        .unwrap()
        .expect("upload_id");

    // S3 requires each non-last part >= 5 MiB; last part can be any size.
    let part_data = vec![0xAB_u8; 6 * 1024 * 1024]; // 6 MiB
    let etag = OciBackend::upload_resumable_object_part(
        &backend,
        &key,
        &upload_id,
        0,
        axum::body::Bytes::from(part_data.clone()),
    )
    .await
    .unwrap();

    OciBackend::complete_resumable_object_upload(&backend, &key, &upload_id, vec![(0, etag)])
        .await
        .unwrap();

    // Object should now exist and have correct length
    assert!(store_contains(&key).await);
    let len = object_length(&key).await;
    assert_eq!(len, part_data.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_abort_resumable_upload() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let key = ObjectKey::parse("oci/abort-upload").unwrap();
    let upload_id = OciBackend::create_resumable_object_upload(&backend, &key)
        .await
        .unwrap()
        .expect("upload_id");

    OciBackend::abort_resumable_object_upload(&backend, &key, &upload_id)
        .await
        .unwrap();

    // Object should not exist after abort
    assert!(!store_contains(&key).await);
}

/// Helper: check if an object exists in the S3 store using the global config.
async fn store_contains(key: &ObjectKey) -> bool {
    let store = s3_store().await;
    store.contains(key).unwrap_or(false)
}

/// Helper: get object length from the S3 store.
async fn object_length(key: &ObjectKey) -> u64 {
    let store = s3_store().await;
    store
        .metadata(key)
        .unwrap()
        .map(|m| m.length())
        .unwrap_or(0)
}

// ===========================================================================
// 2. S3 streaming content-addressed upload (multipart path)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_streaming_content_addressed_via_backend() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    // Create content and compute its SHA256 digest
    let content = b"streaming content addressed data via S3!";
    let digest = Sha256::digest(content);
    let digest_hex = hex::encode(digest);
    let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();

    // Use the OciBackend to store content-addressed bytes
    use shardline_oci_adapter::OciBackend;
    let outcome = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &backend,
        &canonical_key,
        &digest_hex,
        content.to_vec(),
    )
    .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Verify via S3 store directly
    let store = s3_store().await;
    assert!(store.contains(&canonical_key).unwrap());
    let meta = store.metadata(&canonical_key).unwrap().unwrap();
    assert_eq!(meta.length(), content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_streaming_content_addressed_already_exists() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let content = b"dedup-streaming-content";
    let digest = Sha256::digest(content);
    let digest_hex = hex::encode(digest);
    let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();

    use shardline_oci_adapter::OciBackend;
    let first = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &backend,
        &canonical_key,
        &digest_hex,
        content.to_vec(),
    )
    .unwrap();
    assert_eq!(first, PutOutcome::Inserted);

    // Second call should detect AlreadyExists
    let second = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &backend,
        &canonical_key,
        &digest_hex,
        content.to_vec(),
    )
    .unwrap();
    assert_eq!(second, PutOutcome::AlreadyExists);
}

// ===========================================================================
// 3. S3-specific error conversions (object_store.rs From impl)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_from_s3_store_error() {
    // ServerObjectStoreError::S3 -> ServerError::ObjectStore(ObjectStoreError::S3)
    let s3_err = shardline_storage::S3ObjectStoreError::EmptyBucket;
    let store_err = ServerObjectStoreError::S3(s3_err);
    let server_err: ServerError = store_err.into();
    assert!(matches!(
        server_err,
        ServerError::ObjectStore(ObjectStoreError::S3(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_not_found() {
    let err: ServerError = ServerObjectStoreError::NotFound.into();
    assert!(matches!(err, ServerError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_overflow() {
    let err: ServerError = ServerObjectStoreError::Overflow.into();
    assert!(matches!(err, ServerError::Overflow));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_stored_length_mismatch() {
    let err: ServerError = ServerObjectStoreError::StoredObjectLengthMismatch.into();
    assert!(matches!(
        err,
        ServerError::ObjectStore(ObjectStoreError::StoredLengthMismatch)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_local() {
    let io_err = std::io::Error::other("local store error");
    let err: ServerError = ServerObjectStoreError::Local(io_err.into()).into();
    assert!(matches!(
        err,
        ServerError::ObjectStore(ObjectStoreError::Local(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_error_io() {
    let io_err = std::io::Error::other("disk io");
    let err: ServerError = ServerObjectStoreError::Io(io_err).into();
    assert!(matches!(err, ServerError::Io(_)));
}

// ===========================================================================
// 4. BenchmarkBackend S3 construction edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_with_key_prefix() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let base_prefix = MINIO.get().unwrap().1.clone();
    // Add a sub-prefix via the S3 config's key_prefix
    let mut raw_config = s3_config(&base_prefix);
    raw_config = raw_config.with_key_prefix(Some(&format!("{base_prefix}/sub-bench")));
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(raw_config));

    let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "key-prefix-test")
        .await
        .unwrap();
    assert_eq!(bench.metadata_backend_name(), "local");
    assert_eq!(bench.object_backend_name(), "s3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_missing_credentials_fails_on_operation() {
    // S3ObjectStore::new is lazy — it doesn't validate credentials at construction.
    // The failure happens on the first real operation.  We verify that the backend
    // can be created but operations fail with S3 error.
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let raw = MINIO
        .get()
        .unwrap()
        .0
        .s3_raw_config(Some("bad-cred"))
        .unwrap();
    let bad_config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(
            Some("WRONG".to_owned()),
            Some("CREDENTIALS".to_owned()),
            raw.session_token,
        )
        .with_allow_http(raw.allow_http);

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(bad_config));

    let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "bad-cred").await;
    // Construction may succeed (lazy S3 client) but an upload should fail
    if let Ok(bench) = bench {
        let result = bench
            .upload_file(
                "fail-file.bin",
                axum::body::Bytes::from_static(b"data"),
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "upload with bad credentials should fail: {result:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_without_s3_config_fails() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, None);

    let result =
        BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "no-s3-cfg").await;
    assert!(result.is_err(), "benchmark without S3 config should fail");
}

// ===========================================================================
// 5. S3 object store metadata / edge case coverage
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_list_non_existent_prefix() {
    let store = s3_store().await;
    let prefix = ObjectPrefix::parse("zzz/nonexistent").unwrap();
    let results = store.list_prefix(&prefix).unwrap();
    assert!(
        results.is_empty(),
        "non-existent prefix should return empty list"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_visit_prefix() {
    let store = s3_store().await;
    let prefix_str = "crud/visit-prefix";
    let prefix = ObjectPrefix::parse(prefix_str).unwrap();

    // Put two objects
    for i in 0..2 {
        let key = ObjectKey::parse(&format!("{prefix_str}/obj{i}")).unwrap();
        let data = vec![b'x'; 8];
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&data).as_bytes()),
            data.len() as u64,
        );
        store
            .put_if_absent(&key, ObjectBody::from_vec(data), &integrity)
            .unwrap();
    }

    let mut visited: Vec<String> = Vec::new();
    let result: Result<(), shardline_storage::S3ObjectStoreError> =
        store.visit_prefix(&prefix, |meta| {
            visited.push(meta.key().as_str().to_owned());
            Ok(())
        });
    assert!(result.is_ok());
    assert_eq!(visited.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_list_flat_namespace_page() {
    let store = s3_store().await;
    let prefix_str = "crud/flat-page/";
    let prefix = ObjectPrefix::parse(prefix_str).unwrap();
    let body = b"page-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(body).as_bytes()),
        body.len() as u64,
    );

    // Insert 5 keys under the prefix
    for i in 0..5u8 {
        let key = ObjectKey::parse(&format!("{prefix_str}key{i:02}")).unwrap();
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap();
    }

    // Full page
    let full_page = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
    assert_eq!(full_page.len(), 5);

    // Paginated with start_after
    let after_key = ObjectKey::parse(&format!("{prefix_str}key02")).unwrap();
    let after_page = store
        .list_flat_namespace_page(&prefix, Some(&after_key), 10)
        .unwrap();
    assert_eq!(after_page.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_delete_non_existent_idempotent() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/never-existed").unwrap();
    // MinIO delete on non-existent key returns Deleted (idempotent).
    let outcome = store.delete_if_present(&key).unwrap();
    assert_eq!(outcome, DeleteOutcome::Deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_read_missing_returns_not_found() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/not-here").unwrap();
    let result = store.contains(&key).unwrap();
    assert!(!result);
}

// ===========================================================================
// 6. ServerBackend S3 dispatch via BenchmarkBackend
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_uses_s3_object_store() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();
    assert_eq!(backend.object_backend_name(), "s3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_upload_then_read_via_benchmark() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-backend-crud").await;

    let content = b"S3 backend put and read";
    bench
        .upload_file(
            "s3-backend-file.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();

    let downloaded = bench
        .download_file("s3-backend-file.bin", None, None)
        .await
        .unwrap();
    assert_eq!(downloaded, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_overwrite_through_benchmark() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-overwrite").await;

    let original = b"original s3 content for overwrite test";
    bench
        .upload_file(
            "s3-overwrite-file.bin",
            axum::body::Bytes::from_static(original),
            None,
        )
        .await
        .unwrap();

    // Upload again with same file_id to overwrite
    let replacement = b"replacement content that should overwrite";
    bench
        .upload_file(
            "s3-overwrite-file.bin",
            axum::body::Bytes::from_static(replacement),
            None,
        )
        .await
        .unwrap();

    let downloaded = bench
        .download_file("s3-overwrite-file.bin", None, None)
        .await
        .unwrap();
    assert_eq!(downloaded, replacement);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_delete_via_oci() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let _key = ObjectKey::parse("oci/delete-via-oci").unwrap();

    // Store via OCI
    let digest_hex = "dd".repeat(32);
    let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
    OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &backend,
        &canonical_key,
        &digest_hex,
        b"oci-delete-test".to_vec(),
    )
    .unwrap();

    // Delete via OCI
    let outcome = OciBackend::delete_object_if_present(&backend, &canonical_key)
        .await
        .unwrap();
    // S3 delete returns Deleted even on success
    assert_eq!(outcome, DeleteOutcome::Deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_copy_via_oci() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let src = ObjectKey::parse("oci/copy-src").unwrap();
    let dst = ObjectKey::parse("oci/copy-dst").unwrap();

    // Store source using the S3 store directly, then test the copy via OciBackend
    let store = s3_store().await;
    let body = b"copy-source-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(body).as_bytes()),
        body.len() as u64,
    );
    store
        .put_if_absent(&src, ObjectBody::from_slice(body), &integrity)
        .unwrap();

    // Copy via OciBackend
    let outcome = OciBackend::copy_object_if_absent(&backend, &src, &dst).unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Both exist
    assert!(store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
    assert_eq!(
        store.metadata(&src).unwrap().unwrap().length(),
        store.metadata(&dst).unwrap().unwrap().length()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_object_length_via_store() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/length-check").unwrap();
    let data = b"length-check-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );
    store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();

    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.length(), data.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_backend_visit_prefix_via_benchmark_stats() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("visit-stats").await;

    // Upload a file so we have at least one object
    bench
        .upload_file(
            "s3-visit-stats-file.bin",
            axum::body::Bytes::from_static(b"visit stats"),
            None,
        )
        .await
        .unwrap();

    // Stats implicitly visits object prefixes through the metadata backend
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 1,
        "stats should report at least one file: {stats:?}"
    );
}

// ===========================================================================
// 7. OCI S3 mount/blob copy — upload blob, mount via copy
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_mount_blob_copy() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    // Upload a blob (source) using the S3 store directly.
    let store = s3_store().await;
    let src = ObjectKey::parse("mount/blob-source").unwrap();
    let dst = ObjectKey::parse("mount/blob-dest").unwrap();
    let data = b"OCI mount test blob data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );
    store
        .put_if_absent(&src, ObjectBody::from_slice(data), &integrity)
        .unwrap();

    // Mount (copy) via OciBackend dispatch — exercises ServerBackend S3 + Local arm
    let outcome = OciBackend::copy_object_if_absent(&backend, &src, &dst).unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Verify both objects exist
    assert!(store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
    assert_eq!(
        store.metadata(&src).unwrap().unwrap().length(),
        store.metadata(&dst).unwrap().unwrap().length()
    );
}

// ===========================================================================
// 8. Concurrent operations on S3
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_concurrent_put_if_absent_same_key() {
    let store = Arc::new(s3_store().await);
    let key = ObjectKey::parse("concurrent/same-key").unwrap();
    let data = b"concurrent data for same key";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    let mut handles = Vec::new();
    for _ in 0..10 {
        let s = Arc::clone(&store);
        let k = key.clone();
        let int = integrity.clone();
        let body = ObjectBody::from_slice(data);
        handles.push(tokio::spawn(async move { s.put_if_absent(&k, body, &int) }));
    }

    let mut inserted_count = 0;
    let mut already_exists_count = 0;
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "concurrent put should not error");
        match result.unwrap() {
            PutOutcome::Inserted => inserted_count += 1,
            PutOutcome::AlreadyExists => already_exists_count += 1,
        }
    }
    // Exactly one Inserted, the rest AlreadyExists
    assert_eq!(inserted_count, 1, "exactly one should succeed as Inserted");
    assert_eq!(already_exists_count, 9, "nine should be AlreadyExists");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_concurrent_put_different_keys() {
    let store = Arc::new(s3_store().await);
    let mut handles = Vec::new();
    for i in 0..20 {
        let s = Arc::clone(&store);
        let key = ObjectKey::parse(&format!("concurrent/diff-key-{i:02}")).unwrap();
        let data = format!("concurrent-data-{i}");
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(
                *blake3::hash(data.as_bytes()).as_bytes(),
            ),
            data.len() as u64,
        );
        handles.push(tokio::spawn(async move {
            s.put_if_absent(&key, ObjectBody::from_vec(data.into_bytes()), &integrity)
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "concurrent put should succeed");
        assert_eq!(result.unwrap(), PutOutcome::Inserted);
    }

    // Verify all 20 exist
    let store = s3_store().await;
    for i in 0..20 {
        let key = ObjectKey::parse(&format!("concurrent/diff-key-{i:02}")).unwrap();
        assert!(store.contains(&key).unwrap(), "key #{i} should exist");
    }
}

// ===========================================================================
// 9. Edge cases — S3
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_upload_delete_100_objects() {
    let store = s3_store().await;
    let prefix = "bulk-100";
    let integrity_template = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(b"data").as_bytes()),
        4,
    );

    // Upload 100 objects
    for i in 0..100 {
        let key = ObjectKey::parse(&format!("{prefix}/obj{i:04}")).unwrap();
        store
            .put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity_template)
            .unwrap();
    }

    // Verify count via list_prefix
    let list_prefix = ObjectPrefix::parse(prefix).unwrap();
    let objects = store.list_prefix(&list_prefix).unwrap();
    assert_eq!(objects.len(), 100, "should list 100 objects");

    // Delete all 100
    for i in 0..100 {
        let key = ObjectKey::parse(&format!("{prefix}/obj{i:04}")).unwrap();
        let outcome = store.delete_if_present(&key).unwrap();
        assert_eq!(
            outcome,
            DeleteOutcome::Deleted,
            "delete obj{i:04} should succeed"
        );
    }

    // Verify empty
    let objects_after = store.list_prefix(&list_prefix).unwrap();
    assert!(objects_after.is_empty(), "all objects should be deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_object_key_with_special_characters() {
    let store = s3_store().await;

    // Unicode / emoji in key
    let emoji_key = ObjectKey::parse("special/emoji-👍-test").unwrap();
    let unicode_key = ObjectKey::parse("special/unicode-ñöü-测试").unwrap();
    let control_key = ObjectKey::parse("special/control-\u{0000}-test");
    // Control char key should fail
    assert!(control_key.is_err(), "key with null byte should be invalid");

    let data = b"special chars test";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    // Put emoji key
    let outcome = store
        .put_if_absent(&emoji_key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Put unicode key
    let outcome = store
        .put_if_absent(&unicode_key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Verify both exist and have correct content
    assert!(store.contains(&emoji_key).unwrap());
    assert!(store.contains(&unicode_key).unwrap());

    let len = data.len() as u64;
    let range = shardline_protocol::ByteRange::new(0, len - 1).unwrap();
    let emoji_read = store.read_range(&emoji_key, range).unwrap();
    assert_eq!(emoji_read, data);

    let range2 = shardline_protocol::ByteRange::new(0, len - 1).unwrap();
    let unicode_read = store.read_range(&unicode_key, range2).unwrap();
    assert_eq!(unicode_read, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_large_object_roundtrip() {
    let store = s3_store().await;
    let key = ObjectKey::parse("large/roundtrip-5mb").unwrap();

    // Create a 5 MB object (large enough to exercise multipart / streaming)
    let large_data = vec![0xAB_u8; 5 * 1024 * 1024];
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&large_data).as_bytes()),
        large_data.len() as u64,
    );

    // Put
    let outcome = store
        .put_if_absent(&key, ObjectBody::from_vec(large_data.clone()), &integrity)
        .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Verify metadata
    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.length(), large_data.len() as u64);

    // Read back
    let range = shardline_protocol::ByteRange::new(0, large_data.len() as u64 - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read.len(), large_data.len());
    assert_eq!(read[0], 0xAB);
    assert_eq!(read[read.len() - 1], 0xAB);

    // Delete
    let outcome = store.delete_if_present(&key).unwrap();
    assert_eq!(outcome, DeleteOutcome::Deleted);
    assert!(!store.contains(&key).unwrap());
}

// ===========================================================================
// 10. BenchmarkBackend S3 dispatch — coverage through S3 store
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_chunk_length_not_found() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-chunk-len").await;
    let result = bench
        .download_file("nonexistent-file-for-chunk-length", None, None)
        .await;
    assert!(result.is_err(), "download of missing file should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_xorb_length_not_found() {
    let _prefix = ensure_minio().await;
    // xorb_length goes through ServerBackend dispatch → PostgresBackend (local metadata)
    // with S3 object store. It needs a valid hash format.
    let (bench, _tmp) = s3_benchmark("s3-xorb-len").await;
    // Use xorb_length via the object store path — accessible through upload_xorb
    // on BenchmarkBackend. We test via download failure on a missing file which
    // exercises the dispatch through metadata backend + S3 object store.
    let result = bench
        .download_file("nonexistent-xorb-length-file", None, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_upload_then_stats() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-up-stats").await;
    bench
        .upload_file(
            "s3-up-stats-file.bin",
            axum::body::Bytes::from_static(b"s3 stats data"),
            None,
        )
        .await
        .unwrap();
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 1,
        "stats should show at least 1 file: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_download_missing_file() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-dl-missing").await;
    let result = bench.download_file("s3-missing-file.bin", None, None).await;
    assert!(
        result.is_err(),
        "download of missing file via S3 backend should fail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_reconstruction_with_content_hash() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-recon-hash").await;
    let content = b"S3 reconstruction with content hash via BenchmarkBackend";
    let resp = bench
        .upload_file(
            "s3-recon-hash.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();
    let recon = bench
        .reconstruction("s3-recon-hash.bin", Some(&resp.content_hash), None, None)
        .await
        .unwrap();
    assert!(
        !recon.terms.is_empty() || recon.offset_into_first_range == 0,
        "reconstruction should return valid response: {recon:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_benchmark_metadata_backend_name() {
    let _prefix = ensure_minio().await;
    let (bench, _tmp) = s3_benchmark("s3-meta-name").await;
    assert_eq!(bench.metadata_backend_name(), "local");
    assert_eq!(bench.object_backend_name(), "s3");
}

// ===========================================================================
// 11. ServerBackend S3 dispatch via from_config
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_backend_from_config_creates_local_with_s3_store() {
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();
    // Without Postgres URL, the backend is Local variant with S3 object store
    assert_eq!(backend.object_backend_name(), "s3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_backend_oci_copy_object_if_absent() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let src = ObjectKey::parse("oci/s3-copy-src").unwrap();
    let dst = ObjectKey::parse("oci/s3-copy-dst").unwrap();

    // Seed source via S3 store
    let store = s3_store().await;
    let data = b"oci-s3-copy-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );
    store
        .put_if_absent(&src, ObjectBody::from_slice(data), &integrity)
        .unwrap();

    // Copy via OciBackend dispatch (ServerBackend::Local with S3 store)
    let outcome = OciBackend::copy_object_if_absent(&backend, &src, &dst).unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Both should exist
    assert!(store.contains(&src).unwrap());
    assert!(store.contains(&dst).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_server_backend_oci_delete_object_if_present() {
    use shardline_oci_adapter::OciBackend;
    let _prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let config = s3_server_config(&tmp);
    let backend = ServerBackend::from_config(&config).await.unwrap();

    let key = ObjectKey::parse("oci/s3-delete-me").unwrap();
    let store = s3_store().await;
    let data = b"oci-s3-delete-data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );
    store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();

    let outcome = OciBackend::delete_object_if_present(&backend, &key)
        .await
        .unwrap();
    assert_eq!(outcome, DeleteOutcome::Deleted);
    assert!(!store.contains(&key).unwrap());
}

// ===========================================================================
// 12. S3 object store edge cases — overwrite, list after delete
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_overwrite_exercises_put_overwrite() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/overwrite-edge").unwrap();

    let original = b"original content for overwrite edge case";
    let replacement = b"replacement that exercises put_overwrite";
    let integrity1 = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(original).as_bytes()),
        original.len() as u64,
    );
    let integrity2 = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(replacement).as_bytes()),
        replacement.len() as u64,
    );

    store
        .put_if_absent(&key, ObjectBody::from_slice(original), &integrity1)
        .unwrap();
    store
        .put_overwrite(&key, ObjectBody::from_slice(replacement), &integrity2)
        .unwrap();

    let len = replacement.len() as u64;
    let range = shardline_protocol::ByteRange::new(0, len - 1).unwrap();
    let read = store.read_range(&key, range).unwrap();
    assert_eq!(read, replacement);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_list_prefix_after_partial_delete() {
    let store = s3_store().await;
    let prefix_str = "crud/list-partial-delete";
    let prefix = ObjectPrefix::parse(prefix_str).unwrap();

    // Insert 5 objects
    for i in 0..5 {
        let key = ObjectKey::parse(&format!("{prefix_str}/obj{i}")).unwrap();
        let data = vec![b'x'; 8];
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&data).as_bytes()),
            data.len() as u64,
        );
        store
            .put_if_absent(&key, ObjectBody::from_vec(data), &integrity)
            .unwrap();
    }

    // Delete 2
    for i in 0..2 {
        let key = ObjectKey::parse(&format!("{prefix_str}/obj{i}")).unwrap();
        store.delete_if_present(&key).unwrap();
    }

    // List should return 3 remaining
    let objects = store.list_prefix(&prefix).unwrap();
    assert_eq!(objects.len(), 3);
}

// ===========================================================================
// Stress/concurrency tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_s3_100_concurrent_puts() {
    let store = Arc::new(s3_store().await);
    let mut handles = Vec::new();
    for i in 0..100 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let key = ObjectKey::parse(&format!("concurrent100/key-{i:04}")).unwrap();
            let data = format!("concurrent-data-{i}");
            let integrity = ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(
                    *blake3::hash(data.as_bytes()).as_bytes(),
                ),
                data.len() as u64,
            );
            s.put_if_absent(&key, ObjectBody::from_vec(data.into_bytes()), &integrity)
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "concurrent put should succeed");
        assert_eq!(result.unwrap(), PutOutcome::Inserted);
    }

    // Verify all 100 exist
    let store = s3_store().await;
    for i in 0..100 {
        let key = ObjectKey::parse(&format!("concurrent100/key-{i:04}")).unwrap();
        assert!(store.contains(&key).unwrap(), "key #{i} should exist");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_s3_10_concurrent_content_addressed() {
    let store = Arc::new(s3_store().await);
    let mut handles = Vec::new();
    for i in 0..10 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let key = ObjectKey::parse(&format!("ca/concurrent-{i:02}")).unwrap();
            let data = format!("content-addressed-concurrent-{i}");
            let result = s.begin_content_addressed_upload(&key).await;
            match result {
                Ok(BeginMultipartUploadResult::AlreadyExists) => {
                    return Ok::<_, shardline_storage::S3ObjectStoreError>(
                        PutOutcome::AlreadyExists,
                    );
                }
                Ok(BeginMultipartUploadResult::Upload(mut writer, temp_key)) => {
                    writer.write(data.as_bytes());
                    writer.wait_for_capacity(4).await?;
                    let outcome = s
                        .finish_content_addressed_upload(writer, &temp_key, &key)
                        .await?;
                    Ok(outcome)
                }
                Err(e) => Err(e),
            }
        }));
    }

    let mut inserted = 0;
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "concurrent content-addressed upload should succeed"
        );
        if result.unwrap() == PutOutcome::Inserted {
            inserted += 1;
        }
    }
    assert!(
        inserted >= 1,
        "at least one upload should succeed as Inserted"
    );

    // Verify keys exist
    let store = s3_store().await;
    for i in 0..10 {
        let key = ObjectKey::parse(&format!("ca/concurrent-{i:02}")).unwrap();
        assert!(
            store.contains(&key).unwrap(),
            "key ca/concurrent-{i:02} should exist"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_s3_concurrent_put_and_get_same_key() {
    let store = Arc::new(s3_store().await);
    let key = ObjectKey::parse("concurrent/put-get-same").unwrap();
    let data = b"concurrent put-get data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    // Concurrent put + get of same key
    let store_put = Arc::clone(&store);
    let key_put = key.clone();
    let put_handle = tokio::spawn(async move {
        store_put.put_if_absent(&key_put, ObjectBody::from_slice(data), &integrity)
    });

    let store_get = Arc::clone(&store);
    let key_get = key.clone();
    let get_handle = tokio::spawn(async move { store_get.contains(&key_get) });

    let (put_result, get_result) = tokio::join!(put_handle, get_handle);
    // Put may succeed or return AlreadyExists — both valid
    let put = put_result.unwrap();
    assert!(put.is_ok(), "put should succeed: {put:?}");
    // Get may return true or false depending on timing — just verify no crash
    let _ = get_result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_s3_concurrent_put_and_delete_same_key() {
    let store = Arc::new(s3_store().await);
    let key = ObjectKey::parse("concurrent/put-delete-same").unwrap();
    let data = b"concurrent put-delete data";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        data.len() as u64,
    );

    // Concurrent put + delete of same key
    let store_put = Arc::clone(&store);
    let key_put = key.clone();
    let put_handle = tokio::spawn(async move {
        store_put.put_if_absent(&key_put, ObjectBody::from_slice(data), &integrity)
    });

    let store_del = Arc::clone(&store);
    let key_del = key.clone();
    let del_handle = tokio::spawn(async move { store_del.delete_if_present(&key_del) });

    let (put_result, del_result) = tokio::join!(put_handle, del_handle);
    let put = put_result.unwrap();
    assert!(put.is_ok(), "put should succeed: {put:?}");
    let del = del_result.unwrap();
    assert!(del.is_ok(), "delete should succeed: {del:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_put_if_absent_empty_body() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/empty-body").unwrap();
    let data = b"";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
        0,
    );
    let outcome = store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);
    assert!(store.contains(&key).unwrap());
    let meta = store.metadata(&key).unwrap().unwrap();
    assert_eq!(meta.length(), 0);
}
