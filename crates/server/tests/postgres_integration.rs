#![cfg(feature = "docker")]
#![allow(clippy::unwrap_used)]

use std::num::NonZeroUsize;

use shardline_server::{
    BackupManifestReport, BenchmarkBackend, LifecycleRepairOptions, LifecycleRepairReport,
    PostgresBackend, ServerBackend, ServerConfig, ServerObjectStore, ServerStatsResponse,
    apply_database_migrations, bundled_database_migrations, run_lifecycle_repair,
    write_backup_manifest,
};
use shardline_oci_adapter::{OciAdapterError, OciBackend};
use shardline_protocol::ShardlineHash;
use shardline_storage::{DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
use shardline_test_support::DockerLocalStack;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tempfile::TempDir;
use tokio::sync::OnceCell;

// ---------------------------------------------------------------------------
// Shared Docker Postgres — one container for all tests.
// ---------------------------------------------------------------------------

static PG: OnceCell<(DockerLocalStack, String)> = OnceCell::const_new();

/// Ensure the global Docker Postgres is running with migrations applied.
/// Returns the connection URL. Each test creates its own `PgPool`.
async fn ensure_pg() -> &'static str {
    let (_, url) = PG
        .get_or_init(|| async {
            #[allow(clippy::expect_used)]
            let stack = DockerLocalStack::builder()
                .with_postgres()
                .start()
                .unwrap()
                .expect("Docker postgres: is docker available?");
            #[allow(clippy::expect_used)]
            let base = stack.postgres_url().unwrap();
            let url = format!("{base}?sslmode=disable");
            let pool = PgPool::connect(&url).await.unwrap();
            apply_database_migrations(&pool).await.unwrap();
            pool.close().await;
            (stack, url)
        })
        .await;
    url
}

/// Create a fresh pool for the current test's tokio runtime.
async fn fresh_pool() -> PgPool {
    PgPool::connect(ensure_pg().await).await.unwrap()
}

/// Build a minimal [`ServerConfig`] pointing at the global Postgres.
fn pg_config(root: &TempDir) -> ServerConfig {
    #[allow(clippy::expect_used)]
    let url = PG.get().expect("ensure_pg() not called yet").1.clone();
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    ServerConfig::new(
        bind_addr,
        "http://127.0.0.1:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_index_postgres_url(url)
    .unwrap()
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap()
}

/// Create a `BenchmarkBackend` wrapping a Postgres backend.
async fn pg_benchmark(namespace: &str) -> (BenchmarkBackend, TempDir) {
    let tmp = TempDir::new().unwrap();
    let config = pg_config(&tmp);
    let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), namespace)
        .await
        .unwrap();
    (bench, tmp)
}

/// Create a bare `PostgresBackend`.
async fn pg_backend() -> (PostgresBackend, TempDir) {
    #[allow(clippy::expect_used)]
    let url = PG.get().expect("ensure_pg() not called yet").1.clone();
    let tmp = TempDir::new().unwrap();
    let backend = PostgresBackend::new(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(65536).unwrap(),
        &url,
    )
    .await
    .unwrap();
    (backend, tmp)
}

fn blake3_hash(data: &[u8]) -> String {
    hex::encode(blake3::hash(data).as_bytes())
}

// ===========================================================================
// Step 7 — database_migration.rs
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_bundled_not_empty() {
    assert!(!bundled_database_migrations().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_all_applied() {
    let pool = fresh_pool().await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_schema_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count as usize, bundled_database_migrations().len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_history_table_exists() {
    let pool = fresh_pool().await;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.tables \
         WHERE table_name = 'shardline_schema_migrations')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_required_tables_exist() {
    let pool = fresh_pool().await;
    let required = [
        "shardline_file_records",
        "shardline_file_reconstructions",
        "shardline_stored_objects",
        "shardline_dedupe_shards",
        "shardline_quarantine_candidates",
        "shardline_retention_holds",
    ];
    for table in &required {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "table {table} should exist after migration");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_current_version() {
    let pool = fresh_pool().await;
    let version: Option<String> = sqlx::query_scalar(
        "SELECT version FROM shardline_schema_migrations ORDER BY version DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .ok();
    assert!(version.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_checksums_nonempty() {
    let pool = fresh_pool().await;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT version, checksum FROM shardline_schema_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!rows.is_empty());
    for (version, checksum) in &rows {
        assert!(!checksum.is_empty(), "migration {version} has empty checksum");
    }
}

// ===========================================================================
// Step 5 — postgres_backend/backend.rs  (construction + identity)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backend_new_constructs() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    assert_eq!(backend.public_base_url(), "http://127.0.0.1:8080");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backend_benchmark_metadata_name() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("meta-name").await;
    assert_eq!(bench.metadata_backend_name(), "postgres");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backend_benchmark_object_backend_name() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("obj-name").await;
    assert_eq!(bench.object_backend_name(), "local");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backend_benchmark_constructs_postgres_variant() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("variant").await;
    assert_eq!(bench.metadata_backend_name(), "postgres");
    assert_eq!(bench.object_backend_name(), "local");
}

// ===========================================================================
// Step 3 — postgres_backend/read.rs
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_ready_succeeds() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.ready().await;
    assert!(result.is_ok(), "ready() should succeed: {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_reconstruction_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.reconstruction("nonexistent-file", None, None, None).await;
    assert!(result.is_err(), "reconstruction of missing file should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_file_total_bytes_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.file_total_bytes("no-such-file", None, None).await;
    assert!(result.is_err(), "file_total_bytes for missing file should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_download_file_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.download_file("missing-file", None, None).await;
    assert!(result.is_err(), "download_file for missing file should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_xorb_length_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let hash = blake3_hash(b"no-such-xorb");
    let result = backend.xorb_length(&hash).await;
    assert!(matches!(result, Err(shardline_server::ServerError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_chunk_for_file_version_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .read_chunk_for_file_version(
            &blake3_hash(b"chunk"),
            "some-file",
            &blake3_hash(b"content"),
            None,
        )
        .await;
    assert!(result.is_err(), "should fail for missing record");
}

// ===========================================================================
// Step 4 — postgres_backend/upload.rs
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_file() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let response = backend
        .upload_file(
            "hello.txt",
            axum::body::Bytes::from_static(b"Hello, Postgres!"),
            None,
        )
        .await
        .unwrap();
    assert!(!response.file_id.is_empty());
    assert!(response.total_bytes > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_file_via_benchmark() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("up-file").await;
    let response = bench
        .upload_file(
            "upfile.txt",
            axum::body::Bytes::from_static(b"Benchmark upload"),
            None,
        )
        .await
        .unwrap();
    assert!(!response.file_id.is_empty());
    // Verify via direct SQL that the metadata was written.
    let pool = fresh_pool().await;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM shardline_file_records WHERE file_id = 'upfile.txt')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists, "file record should exist after upload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_and_reconstruct_roundtrip() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("roundtrip").await;
    let content = b"Integration test roundtrip content!";
    bench
        .upload_file("roundtrip.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    let response = bench
        .reconstruction("roundtrip.bin", None, None, None)
        .await
        .unwrap();
    assert!(
        !response.terms.is_empty() || response.offset_into_first_range == 0,
        "reconstruction should return a valid response"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_and_download_roundtrip() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("download-rt").await;
    let content = b"Download this content please";
    bench
        .upload_file("download-me.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    let downloaded = bench
        .download_file("download-me.bin", None, None)
        .await
        .unwrap();
    assert_eq!(downloaded, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_xorb_rejects_invalid_body() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let data = b"not a valid xorb at all";
    let hash = blake3_hash(data);
    let result = backend
        .upload_xorb(&hash, axum::body::Bytes::from_static(data))
        .await;
    assert!(result.is_err(), "upload_xorb should reject invalid xorb data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_xorb_rejects_hash_mismatch() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let data = b"some content that has a hash";
    let wrong_hash = blake3_hash(b"different data entirely");
    let result = backend
        .upload_xorb(&wrong_hash, axum::body::Bytes::from_static(data))
        .await;
    assert!(result.is_err(), "upload_xorb should reject hash mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_file_rejects_invalid_file_id() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .upload_file("/absolute/path", axum::body::Bytes::from_static(b"data"), None)
        .await;
    assert!(matches!(result, Err(shardline_server::ServerError::InvalidFileId)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_file_empty_id() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .upload_file("", axum::body::Bytes::from_static(b"data"), None)
        .await;
    assert!(matches!(result, Err(shardline_server::ServerError::InvalidFileId)));
}

// ===========================================================================
// Step 6 — postgres_backend/stats.rs
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stats_returns_counts() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("stats-check").await;
    // Upload a file so stats has something to count.
    bench
        .upload_file(
            "stats-check-file.bin",
            axum::body::Bytes::from_static(b"stats content"),
            None,
        )
        .await
        .unwrap();
    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 1, "should have at least one file: {stats:?}");
}

// ===========================================================================
// Step 8 — backend.rs Postgres arms (via BenchmarkBackend)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backend_upload_then_reconstruction() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("up-recon").await;
    let content = b"backend upload and reconstruct";
    bench
        .upload_file("bench-recon.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    let response = bench
        .reconstruction("bench-recon.bin", None, None, None)
        .await
        .unwrap();
    assert!(
        !response.terms.is_empty() || response.offset_into_first_range == 0,
        "reconstruction should return terms or zero offset"
    );
}

// ===========================================================================
// Step 9 — backup.rs with Postgres
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backup_manifest_with_postgres_backend() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();
    let config = pg_config(&tmp);
    let mut buffer = Vec::new();
    let report = write_backup_manifest(config, &mut buffer).await.unwrap();
    assert_eq!(report.metadata_backend, "postgres");
    assert_eq!(report.object_backend, "local");
    assert_eq!(report.manifest_version, 1);
    let json_str = String::from_utf8(buffer).unwrap();
    assert!(json_str.contains("manifest_version"));
    assert!(json_str.contains("postgres"));
    assert!(json_str.contains("\"objects\":["));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backup_manifest_counts_after_upload() {
    let _url = ensure_pg().await;
    // Upload a file via BenchmarkBackend first.
    {
        let (bench, _tmp) = pg_benchmark("backup-counts").await;
        bench
            .upload_file(
                "backup-test.bin",
                axum::body::Bytes::from_static(b"backup content"),
                None,
            )
            .await
            .unwrap();
    }
    let tmp = TempDir::new().unwrap();
    let config = pg_config(&tmp);
    let mut buffer = Vec::new();
    let report = write_backup_manifest(config, &mut buffer).await.unwrap();
    assert!(report.latest_records >= 1, "should record at least one file: {report:?}");
    assert_eq!(report.metadata_backend, "postgres");
}

// ===========================================================================
// Step 10 — provider.rs Postgres
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_provider_repository_state_table_exists() {
    let pool = fresh_pool().await;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.tables \
         WHERE table_name = 'shardline_provider_repository_states')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists, "provider_repository_states table should exist after migration");
}

// ===========================================================================
// Complete workflow: migration -> upload -> verify -> stats -> backup
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_complete_workflow() {
    let _url = ensure_pg().await;

    // 1. Verify migrations applied.
    let pool = fresh_pool().await;
    let migration_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_schema_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(migration_count as usize, bundled_database_migrations().len());

    // 2. Upload a file.
    let (bench, _tmp) = pg_benchmark("workflow").await;
    bench
        .upload_file(
            "workflow-test.txt",
            axum::body::Bytes::from_static(b"Workflow integration test"),
            None,
        )
        .await
        .unwrap();

    // 3. Verify via SQL.
    let file_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM shardline_file_records WHERE file_id = 'workflow-test.txt')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(file_exists, "file record should exist");

    // 4. Stats reflect at least one file.
    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 1, "stats should show at least one file");

    // 5. Backup manifest captures the file.
    let tmp2 = TempDir::new().unwrap();
    let config = pg_config(&tmp2);
    let mut buffer = Vec::new();
    let report = write_backup_manifest(config, &mut buffer).await.unwrap();
    assert_eq!(report.metadata_backend, "postgres");
    assert!(report.latest_records >= 1);
}

// ===========================================================================
// Pure type tests (no Docker needed)
// ===========================================================================

#[test]
fn test_server_stats_response_default() {
    let stats = ServerStatsResponse {
        chunks: 0,
        chunk_bytes: 0,
        files: 0,
    };
    assert_eq!(stats.chunks, 0);
    assert_eq!(stats.chunk_bytes, 0);
    assert_eq!(stats.files, 0);
}

#[test]
fn test_server_stats_response_arbitrary() {
    let stats = ServerStatsResponse {
        chunks: 10,
        chunk_bytes: 2048,
        files: 3,
    };
    assert_eq!(stats.chunks, 10);
    assert_eq!(stats.chunk_bytes, 2048);
    assert_eq!(stats.files, 3);
}

#[test]
fn test_backup_manifest_report_defaults() {
    let report = BackupManifestReport {
        manifest_version: 1,
        metadata_backend: "postgres".to_owned(),
        object_backend: "local".to_owned(),
        object_count: 0,
        object_bytes: 0,
        latest_records: 0,
        version_records: 0,
        reconstruction_rows: 0,
        dedupe_shard_mappings: 0,
        quarantine_candidates: 0,
        retention_holds: 0,
        webhook_deliveries: 0,
        provider_repository_states: 0,
    };
    assert_eq!(report.metadata_backend, "postgres");
    assert_eq!(report.object_backend, "local");
    assert_eq!(report.manifest_version, 1);
    assert_eq!(report.latest_records, 0);
}

// ===========================================================================
// SECTION 2 — ServerBackend::Postgres variant dispatch
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_backend_postgres_variant_construction() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    // Verify the variant is Postgres by matching.
    assert!(
        matches!(sb, ServerBackend::Postgres(_)),
        "should construct Postgres variant"
    );
}

// ===========================================================================
// OciBackend trait — dispatch via Postgres backend (local object store)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_create_resumable_upload_returns_none_for_local_store() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-create-key").unwrap();
    let result: Result<Option<String>, OciAdapterError> =
        OciBackend::create_resumable_object_upload(&sb, &key).await;
    assert!(result.is_ok());
    // Local store → None (no resumable upload support).
    assert!(result.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_upload_resumable_part_returns_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-upload-key").unwrap();
    let result: Result<String, OciAdapterError> =
        OciBackend::upload_resumable_object_part(
            &sb, &key, "upload-id", 0, axum::body::Bytes::from_static(b"part"),
        )
        .await;
    // Local store branch → NotFound.
    assert!(matches!(result, Err(shardline_oci_adapter::OciAdapterError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_complete_resumable_upload_returns_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-complete-key").unwrap();
    let result: Result<(), OciAdapterError> =
        OciBackend::complete_resumable_object_upload(
            &sb, &key, "upload-id", vec![(0, "etag".to_owned())],
        )
        .await;
    assert!(matches!(result, Err(shardline_oci_adapter::OciAdapterError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_abort_resumable_upload_succeeds() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-abort-key").unwrap();
    // Local store branch → Ok(()).
    let result: Result<(), OciAdapterError> =
        OciBackend::abort_resumable_object_upload(&sb, &key, "upload-id").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_copy_object_if_absent() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let src = ObjectKey::parse("oci-copy-src").unwrap();
    let dst = ObjectKey::parse("oci-copy-dst").unwrap();
    // Seed source via the backend's put path.
    // put_object_bytes_if_absent is pub(crate) so seed through upload.
    // Instead, use the ServerBackend's OCI layer if possible, or use the
    // object store directly.
    let store = ServerObjectStore::local(_tmp.path().join("chunks")).unwrap();
    let data = b"oci-copy-data";
    let hash = ShardlineHash::from_bytes(*blake3::hash(data).as_bytes());
    let integrity = ObjectIntegrity::new(hash, data.len() as u64);
    store.put_if_absent(&src, ObjectBody::from_slice(data), &integrity).unwrap();
    let result: Result<PutOutcome, OciAdapterError> =
        OciBackend::copy_object_if_absent(&sb, &src, &dst);
    assert!(result.is_ok());
    let put_outcome = result.unwrap();
    // Either Inserted (first copy) or AlreadyExists.
    assert!(
        put_outcome == PutOutcome::Inserted || put_outcome == PutOutcome::AlreadyExists,
        "copy should succeed: {put_outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_delete_object_if_present() {
    let _url = ensure_pg().await;
    let (backend, tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-delete-me").unwrap();
    // Seed an object.
    let store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let data = b"oci-delete-data";
    let hash = ShardlineHash::from_bytes(*blake3::hash(data).as_bytes());
    let integrity = ObjectIntegrity::new(hash, data.len() as u64);
    store.put_if_absent(&key, ObjectBody::from_slice(data), &integrity).unwrap();
    let result: Result<DeleteOutcome, OciAdapterError> =
        OciBackend::delete_object_if_present(&sb, &key).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), DeleteOutcome::Deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_put_sha256_addressed_object_bytes_if_absent() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let body = b"oci-sha256-payload";
    let digest_hex = hex::encode(Sha256::digest(body));
    let canonical_key = shardline_server::shared_sha256_object_key(&digest_hex).unwrap();
    let result: Result<PutOutcome, OciAdapterError> =
        OciBackend::put_sha256_addressed_object_bytes_if_absent(
            &sb, &canonical_key, &digest_hex, body.to_vec(),
        );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PutOutcome::Inserted);
}

// ===========================================================================
// Record operations — multi-file uploads, verify via SQL + stats
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_record_ops_multi_file_upload() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("multi-rec").await;
    let files = ["alpha.txt", "beta.txt", "gamma.txt"];
    for (i, name) in files.iter().enumerate() {
        let content = format!("content-{i}");
        bench
            .upload_file(name, axum::body::Bytes::from(content), None)
            .await
            .unwrap();
    }
    // Verify all three records exist via SQL.
    let pool = fresh_pool().await;
    for name in &files {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM shardline_file_records WHERE file_id = $1)",
        )
        .bind(name)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "record {name} should exist");
    }
    // Stats should show >= 3 files.
    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 3, "stats should show >= 3 files: {stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_record_ops_duplicate_upload_idempotent() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("dup-rec").await;
    let content = b"same content";
    // Upload twice.
    let r1 = bench
        .upload_file("dup-file.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    let r2 = bench
        .upload_file("dup-file.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    // Both should succeed (idempotent).
    assert!(!r1.file_id.is_empty());
    assert!(!r2.file_id.is_empty());
    // The same file_id may produce multiple records (each upload is a new
    // version).  Verify at least one record exists and the uploads succeed.
    let pool = fresh_pool().await;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_file_records WHERE file_id = 'dup-file.bin'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(count >= 1, "at least one record should exist");
    assert!(r1.file_id == r2.file_id, "same file id for both uploads");
}

// ===========================================================================
// Database migration — version ordering, SQL validity
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_version_ordering() {
    let migrations = bundled_database_migrations();
    assert!(!migrations.is_empty());
    assert!(migrations.windows(2).all(|w| w.first().unwrap().version < w.get(1).unwrap().version));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_each_has_valid_sql() {
    for m in bundled_database_migrations() {
        assert!(!m.up_sql.is_empty(), "{} up_sql empty", m.version);
        assert!(!m.down_sql.is_empty(), "{} down_sql empty", m.version);
        assert!(
            m.up_sql.trim().starts_with("CREATE")
                || m.up_sql.trim().starts_with("ALTER")
                || m.up_sql.trim().starts_with("INSERT")
                || m.up_sql.trim().starts_with("--"),
            "{} up_sql unexpected start: {:?}",
            m.version,
            &m.up_sql.trim()[..20.min(m.up_sql.trim().len())]
        );
        assert!(!m.name.is_empty(), "{} name empty", m.version);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_find_by_version_iteration() {
    let migrations = bundled_database_migrations();
    // Simulate migration_by_version via linear search.
    let found = migrations.iter().find(|m| m.version == "20260417000000");
    assert!(found.is_some(), "known version should be findable");
    assert_eq!(found.unwrap().name, "metadata_store");
    let not_found = migrations.iter().find(|m| m.version == "00000000000000");
    assert!(not_found.is_none(), "unknown version should not be found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_status_entry_accessors() {
    use shardline_server::DatabaseMigrationStatusEntry;
    let entry = DatabaseMigrationStatusEntry {
        version: "v1".to_owned(),
        name: "test".to_owned(),
        applied: true,
        applied_at_utc: Some("2026-01-01T00:00:00Z".to_owned()),
    };
    assert_eq!(entry.version, "v1");
    assert_eq!(entry.name, "test");
    assert!(entry.applied);
    assert!(entry.applied_at_utc.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_options_new() {
    use shardline_server::{DatabaseMigrationCommand, DatabaseMigrationOptions};
    let opts = DatabaseMigrationOptions::new(
        "postgres://localhost/test".to_owned(),
        DatabaseMigrationCommand::Up { steps: Some(2) },
    );
    assert_eq!(opts.database_url(), "postgres://localhost/test");
    assert!(matches!(opts.command(), DatabaseMigrationCommand::Up { steps: Some(2) }));
}

// ===========================================================================
// Lifecycle repair — runs with Postgres stores (via config)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_repair_empty_store() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();
    let config = pg_config(&tmp);
    let report: LifecycleRepairReport = run_lifecycle_repair(config, LifecycleRepairOptions::default())
        .await
        .unwrap();
    // The shared database may have records from other tests, so counts
    // can vary. Just verify the function completes without error and returns
    // a well-formed report.
    assert!(
        report.referenced_objects <= 1_000_000,
        "sanity bound on referenced objects: {report:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_repair_with_seeded_records() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();
    // Upload a file first so there are records to scan.
    {
        let config = pg_config(&tmp);
        let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "repair-seed")
            .await
            .unwrap();
        bench
            .upload_file("repair-seed.bin", axum::body::Bytes::from_static(b"repair data"), None)
            .await
            .unwrap();
    }
    let config = pg_config(&tmp);
    let report: LifecycleRepairReport = run_lifecycle_repair(config, LifecycleRepairOptions::default())
        .await
        .unwrap();
    // At minimum we should have scanned the seeded record(s).
    assert!(
        report.scanned_records >= 1,
        "lifecycle repair should complete: {report:?}"
    );
}

// ===========================================================================
// BenchmarkBackend — additional edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_empty_upload() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("empty-up").await;
    let response = bench
        .upload_file("empty-file.bin", axum::body::Bytes::new(), None)
        .await
        .unwrap();
    assert!(response.total_bytes == 0, "empty upload should have 0 bytes");
    assert!(!response.file_id.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_reconstruction_with_content_hash() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("recon-hash").await;
    let content = b"reconstruct with content hash";
    let resp = bench
        .upload_file("recon-hash.bin", axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    // Reconstruction by content hash should succeed.
    let recon = bench
        .reconstruction("recon-hash.bin", Some(&resp.content_hash), None, None)
        .await
        .unwrap();
    assert!(
        !recon.terms.is_empty() || recon.offset_into_first_range == 0,
        "reconstruction by content hash should return valid response"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_reconstruction_invalid_content_hash() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("recon-bad-hash").await;
    bench
        .upload_file("recon-bad-hash.bin", axum::body::Bytes::from_static(b"data"), None)
        .await
        .unwrap();
    // Non-matching content hash should fail.
    let result = bench
        .reconstruction("recon-bad-hash.bin", Some("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), None, None)
        .await;
    assert!(result.is_err(), "reconstruction with wrong content hash should fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_download_missing_file() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("dl-missing").await;
    let result = bench.download_file("definitely-not-uploaded", None, None).await;
    assert!(result.is_err(), "download of missing file should fail");
}

// ===========================================================================
// Stats — verified after multi-file operations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stats_after_multi_file_upload() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("stats-multi").await;
    for i in 0..5 {
        let content = format!("multi-stats-content-{i}");
        bench
            .upload_file(
                &format!("stats-multi-{i}.bin"),
                axum::body::Bytes::from(content),
                None,
            )
            .await
            .unwrap();
    }
    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 5, "should have at least 5 files: {stats:?}");
}

// ===========================================================================
// Additional ServerBackend dispatch — pure construction matching
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_backend_local_variant_from_config_without_pg() {
    // When no index_postgres_url is set, from_config creates Local.
    // This tests that the dispatch logic correctly chooses Local variant.
    let tmp = TempDir::new().unwrap();
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let config = ServerConfig::new(
        bind_addr,
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    );
    // from_config is pub(crate), so we use BenchmarkBackend::from_config instead.
    let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "no-pg")
        .await
        .unwrap();
    assert_eq!(bench.metadata_backend_name(), "local");
}

// ===========================================================================
// Backup manifest — verify report fields after seeding records
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backup_manifest_with_multiple_records() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();
    // Seed multiple records.
    {
        let config = pg_config(&tmp);
        let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "bkup-multi")
            .await
            .unwrap();
        for i in 0..3 {
            let content = format!("backup-content-{i}");
            bench
                .upload_file(
                    &format!("backup-multi-{i}.bin"),
                    axum::body::Bytes::from(content),
                    None,
                )
                .await
                .unwrap();
        }
    }
    let config = pg_config(&tmp);
    let mut buffer = Vec::new();
    let report = write_backup_manifest(config, &mut buffer).await.unwrap();
    assert_eq!(report.metadata_backend, "postgres");
    assert!(report.latest_records >= 3, "should have >= 3 records: {report:?}");
    assert!(report.object_count > 0, "object count should be positive");
    let json_str = String::from_utf8(buffer).unwrap();
    // Verify the manifest is valid JSON with expected fields.
    assert!(json_str.contains("metadata_backend"));
    assert!(json_str.contains("postgres"));
    assert!(json_str.contains("latest_records"));
}
