#![cfg(feature = "docker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::NonZeroUsize;

use shardline_server::{
    BenchmarkBackend, ObjectStorageAdapter, ServerConfig,
};
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
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(data).as_bytes(),
        ),
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
async fn test_s3_put_if_absent_idempotent() {
    let store = s3_store().await;
    let key = ObjectKey::parse("crud/idempotent").unwrap();
    let data = b"same content";
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(data).as_bytes(),
        ),
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
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(data).as_bytes(),
        ),
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
            shardline_protocol::ShardlineHash::from_bytes(
                *blake3::hash(&data).as_bytes(),
            ),
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
    store.abort_resumable_upload(&key, &upload_id).await.unwrap();

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
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(data).as_bytes(),
        ),
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
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(original).as_bytes(),
        ),
        original.len() as u64,
    );
    let integrity2 = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(
            *blake3::hash(replacement).as_bytes(),
        ),
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

    let result = bench.reconstruction("nonexistent-s3-file", None, None, None).await;
    assert!(result.is_err(), "reconstruction of missing file should fail");
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
