#![cfg(feature = "docker")]
#![allow(clippy::unwrap_used)]

use std::num::NonZeroUsize;

use shardline_server::{
    BackupManifestReport, BenchmarkBackend, PostgresBackend, ServerConfig, ServerStatsResponse,
    apply_database_migrations, bundled_database_migrations, write_backup_manifest,
};
use shardline_test_support::DockerLocalStack;
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
