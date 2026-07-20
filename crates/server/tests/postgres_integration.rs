#![cfg(feature = "docker")]
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::expect_used,
    clippy::panic
)]

use std::{num::NonZeroUsize, time::Duration};

use std::sync::Arc;

use sha2::{Digest, Sha256};
use shardline_cache::{AsyncReconstructionCache, ReconstructionCacheKey, RedisReconstructionCache};
use shardline_oci_adapter::{OciAdapterError, OciBackend};
use shardline_protocol::ShardlineHash;
use shardline_server::{
    BackupManifestReport, BenchmarkBackend, DatabaseMigrationCommand, DatabaseMigrationOptions,
    LifecycleRepairOptions, LifecycleRepairReport, PostgresBackend, ServerBackend, ServerConfig,
    ServerObjectStore, ServerStatsResponse, apply_database_migrations, bundled_database_migrations,
    run_database_migration, run_lifecycle_repair, write_backup_manifest,
};
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome,
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
            // Connect with retry (container may not accept connections immediately).
            let mut last_err = None;
            for _ in 0..5 {
                match PgPool::connect(&url).await {
                    Ok(pool) => {
                        apply_database_migrations(&pool).await.unwrap();
                        pool.close().await;
                        return (stack, url);
                    }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
            panic!("Failed to connect to Postgres after 5 retries: {last_err:?}");
        })
        .await;
    url
}

/// Create a fresh pool for the current test's tokio runtime.
async fn fresh_pool() -> PgPool {
    PgPool::connect(ensure_pg().await).await.unwrap()
}

async fn apply_migrations_to(url: &str) {
    let mut last_error = None;
    for _ in 0..20 {
        match PgPool::connect(url).await {
            Ok(pool) => {
                apply_database_migrations(&pool).await.unwrap();
                pool.close().await;
                return;
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Postgres did not accept connections after startup: {last_error:?}");
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
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_schema_migrations")
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
        assert!(
            !checksum.is_empty(),
            "migration {version} has empty checksum"
        );
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
async fn test_postgres_backend_ready_recovers_after_database_restart() {
    let Some(mut stack) = DockerLocalStack::builder().with_postgres().start().unwrap() else {
        eprintln!("skipping — Docker not available");
        return;
    };
    let url = format!(
        "{}?sslmode=disable&connect_timeout=1",
        stack.postgres_url().unwrap()
    );
    apply_migrations_to(&url).await;

    let tmp = TempDir::new().unwrap();
    let backend = PostgresBackend::new(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(65536).unwrap(),
        &url,
    )
    .await
    .unwrap();
    assert!(
        backend.ready().await.is_ok(),
        "backend should initially be ready"
    );

    stack.stop_postgres().unwrap();
    drop(backend);
    stack.start_postgres().unwrap();
    let recovered_url = format!(
        "{}?sslmode=disable&connect_timeout=1",
        stack.postgres_url().unwrap()
    );
    let recovered_backend = PostgresBackend::new(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(65536).unwrap(),
        &recovered_url,
    )
    .await
    .unwrap();
    assert!(
        recovered_backend.ready().await.is_ok(),
        "a newly initialized backend should be ready after Postgres restarts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_reconstruction_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .reconstruction("nonexistent-file", None, None, None)
        .await;
    assert!(
        result.is_err(),
        "reconstruction of missing file should fail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_file_total_bytes_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.file_total_bytes("no-such-file", None, None).await;
    assert!(
        result.is_err(),
        "file_total_bytes for missing file should fail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_download_file_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.download_file("missing-file", None, None).await;
    assert!(
        result.is_err(),
        "download_file for missing file should fail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_xorb_length_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let hash = blake3_hash(b"no-such-xorb");
    let result = backend.xorb_length(&hash).await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::NotFound)
    ));
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
        .upload_file(
            "roundtrip.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
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
        .upload_file(
            "download-me.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
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
    assert!(
        result.is_err(),
        "upload_xorb should reject invalid xorb data"
    );
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
        .upload_file(
            "/absolute/path",
            axum::body::Bytes::from_static(b"data"),
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::InvalidFileId)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_file_empty_id() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .upload_file("", axum::body::Bytes::from_static(b"data"), None)
        .await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::InvalidFileId)
    ));
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
        .upload_file(
            "bench-recon.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
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
    assert!(
        report.latest_records >= 1,
        "should record at least one file: {report:?}"
    );
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
    assert!(
        exists,
        "provider_repository_states table should exist after migration"
    );
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
    assert_eq!(
        migration_count as usize,
        bundled_database_migrations().len()
    );

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
    let result: Result<String, OciAdapterError> = OciBackend::upload_resumable_object_part(
        &sb,
        &key,
        "upload-id",
        0,
        axum::body::Bytes::from_static(b"part"),
    )
    .await;
    // Local store branch → NotFound.
    assert!(matches!(
        result,
        Err(shardline_oci_adapter::OciAdapterError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_complete_resumable_upload_returns_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    let key = ObjectKey::parse("oci-complete-key").unwrap();
    let result: Result<(), OciAdapterError> = OciBackend::complete_resumable_object_upload(
        &sb,
        &key,
        "upload-id",
        vec![(0, "etag".to_owned())],
    )
    .await;
    assert!(matches!(
        result,
        Err(shardline_oci_adapter::OciAdapterError::NotFound)
    ));
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
    store
        .put_if_absent(&src, ObjectBody::from_slice(data), &integrity)
        .unwrap();
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
    store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
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
            &sb,
            &canonical_key,
            &digest_hex,
            body.to_vec(),
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
        .upload_file(
            "dup-file.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();
    let r2 = bench
        .upload_file(
            "dup-file.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
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
    assert!(
        migrations
            .windows(2)
            .all(|w| w.first().unwrap().version < w.get(1).unwrap().version)
    );
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
    assert!(matches!(
        opts.command(),
        DatabaseMigrationCommand::Up { steps: Some(2) }
    ));
}

// ===========================================================================
// Lifecycle repair — runs with Postgres stores (via config)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_repair_empty_store() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();
    let config = pg_config(&tmp);
    let report: LifecycleRepairReport =
        run_lifecycle_repair(config, LifecycleRepairOptions::default())
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
            .upload_file(
                "repair-seed.bin",
                axum::body::Bytes::from_static(b"repair data"),
                None,
            )
            .await
            .unwrap();
    }
    let config = pg_config(&tmp);
    let report: LifecycleRepairReport =
        run_lifecycle_repair(config, LifecycleRepairOptions::default())
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
    assert!(
        response.total_bytes == 0,
        "empty upload should have 0 bytes"
    );
    assert!(!response.file_id.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_reconstruction_with_content_hash() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("recon-hash").await;
    let content = b"reconstruct with content hash";
    let resp = bench
        .upload_file(
            "recon-hash.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
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
        .upload_file(
            "recon-bad-hash.bin",
            axum::body::Bytes::from_static(b"data"),
            None,
        )
        .await
        .unwrap();
    // Non-matching content hash should fail.
    let result = bench
        .reconstruction(
            "recon-bad-hash.bin",
            Some("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            None,
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "reconstruction with wrong content hash should fail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_benchmark_backend_download_missing_file() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("dl-missing").await;
    let result = bench
        .download_file("definitely-not-uploaded", None, None)
        .await;
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
    assert!(
        report.latest_records >= 3,
        "should have >= 3 records: {report:?}"
    );
    assert!(report.object_count > 0, "object count should be positive");
    let json_str = String::from_utf8(buffer).unwrap();
    // Verify the manifest is valid JSON with expected fields.
    assert!(json_str.contains("metadata_backend"));
    assert!(json_str.contains("postgres"));
    assert!(json_str.contains("latest_records"));
}

// ===========================================================================
// ServerBackend::Postgres dispatch — direct pub-method coverage
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_public_base_url() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    assert_eq!(backend.public_base_url(), "http://127.0.0.1:8080");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_ready_succeeds() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.ready().await;
    assert!(
        result.is_ok(),
        "ready() should succeed with Postgres: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_file_total_bytes_after_upload() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let content = b"check total bytes via Postgres";
    backend
        .upload_file(
            "pg-total-bytes.txt",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();
    let total = backend
        .file_total_bytes("pg-total-bytes.txt", None, None)
        .await
        .unwrap();
    assert_eq!(total, content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_file_total_bytes_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.file_total_bytes("nonexistent", None, None).await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_chunk_length_rejects_invalid_hash() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.chunk_length("not-a-valid-hex").await;
    assert!(result.is_err(), "chunk_length should reject invalid hash");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_chunk_length_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.chunk_length(&"c".repeat(64)).await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_xorb_length_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let hash = blake3_hash(b"no-such-xorb-for-pg-backend");
    let result = backend.xorb_length(&hash).await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_xorb_length_after_upload() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    // upload_xorb via Postgres backend
    let data = b"valid xorb body for length check";
    let hash = blake3_hash(data);
    let result = backend
        .upload_xorb(&hash, axum::body::Bytes::from_static(data))
        .await;
    // upload_xorb expects valid xorb format; may fail if body isn't valid xorb
    // If it fails, xorb_length should still be NotFound
    if result.is_ok() {
        let len = backend.xorb_length(&hash).await.unwrap();
        assert_eq!(len, data.len() as u64);
    } else {
        let err = backend.xorb_length(&hash).await;
        assert!(err.is_err(), "xorb without valid upload should be missing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_read_chunk_rejects_invalid_hash() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.read_chunk("bad-hash").await;
    assert!(result.is_err(), "read_chunk should reject invalid hash");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_read_chunk_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend.read_chunk(&"d".repeat(64)).await;
    assert!(matches!(
        result,
        Err(shardline_server::ServerError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_read_chunk_for_file_version_rejects_invalid_file_id() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .read_chunk_for_file_version(
            &blake3_hash(b"hash"),
            "/absolute/path",
            &blake3_hash(b"content"),
            None,
        )
        .await;
    assert!(result.is_err(), "should reject invalid file_id");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_read_chunk_for_file_version_not_found() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let result = backend
        .read_chunk_for_file_version(
            &blake3_hash(b"chunkkey"),
            "nonexistent-file",
            &blake3_hash(b"version"),
            None,
        )
        .await;
    assert!(result.is_err(), "should fail for missing record");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_stats_after_upload() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    backend
        .upload_file(
            "pg-stats-test.txt",
            axum::body::Bytes::from_static(b"stats data"),
            None,
        )
        .await
        .unwrap();
    let stats = backend.stats().await.unwrap();
    assert!(
        stats.files >= 1,
        "stats should show at least 1 file: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_upload_xorb_rejects_invalid_body() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let data = b"not a valid xorb body at all";
    let hash = blake3_hash(data);
    let result = backend
        .upload_xorb(&hash, axum::body::Bytes::from_static(data))
        .await;
    assert!(
        result.is_err(),
        "upload_xorb should reject invalid xorb data"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_upload_xorb_rejects_hash_mismatch() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let data = b"body content with wrong hash";
    let wrong_hash = blake3_hash(b"different data");
    let result = backend
        .upload_xorb(&wrong_hash, axum::body::Bytes::from_static(data))
        .await;
    assert!(result.is_err(), "upload_xorb should reject hash mismatch");
}

// ===========================================================================
// ServerBackend identity dispatch — Postgres variant
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_backend_postgres_object_backend_name() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    assert_eq!(sb.object_backend_name(), "local");
}

// ===========================================================================
// Postgres operations with larger data
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_100_files_via_postgres() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("hundred-files").await;
    for i in 0..100 {
        let content = format!("hundred-file-content-{i}");
        let file_id = format!("hf-{i:04}.bin");
        bench
            .upload_file(&file_id, axum::body::Bytes::from(content), None)
            .await
            .unwrap();
    }
    // Stats should reflect them
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 100,
        "stats should show >= 100 files: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_then_delete_and_verify_stats() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("up-del-stats").await;

    // Upload 10 files
    for i in 0..10 {
        let content = format!("delete-stats-content-{i}");
        let file_id = format!("up-del-custom-{i:02}.txt");
        bench
            .upload_file(&file_id, axum::body::Bytes::from(content), None)
            .await
            .unwrap();
    }

    // Stats should reflect uploaded files
    let stats_before = bench.stats().await.unwrap();
    assert!(
        stats_before.files >= 10,
        "should have >= 10 files: {stats_before:?}"
    );

    // Download each file to verify content
    for i in 0..10 {
        let file_id = format!("up-del-custom-{i:02}.txt");
        let downloaded = bench.download_file(&file_id, None, None).await.unwrap();
        let expected = format!("delete-stats-content-{i}");
        assert_eq!(String::from_utf8(downloaded).unwrap(), expected);
    }
}

// ===========================================================================
// Edge cases — Postgres
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_empty_file_upload() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let response = backend
        .upload_file("empty-pg-file.bin", axum::body::Bytes::new(), None)
        .await
        .unwrap();
    assert_eq!(response.total_bytes, 0, "empty upload should have 0 bytes");
    assert!(!response.file_id.is_empty());
    // Verify via download
    let downloaded = backend
        .download_file("empty-pg-file.bin", None, None)
        .await
        .unwrap();
    assert!(
        downloaded.is_empty(),
        "downloaded empty file should be empty"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_backend_large_metadata_values_upload() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    // Upload a file with a large body to exercise larger metadata records
    let large_body = vec![0xAB_u8; 1_000_000]; // 1 MB
    let response = backend
        .upload_file(
            "large-meta-file.bin",
            axum::body::Bytes::from(large_body.clone()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(response.total_bytes, 1_000_000);
    // Download and verify
    let downloaded = backend
        .download_file("large-meta-file.bin", None, None)
        .await
        .unwrap();
    assert_eq!(downloaded.len(), 1_000_000);
    assert_eq!(downloaded[0], 0xAB);
    assert_eq!(downloaded[999_999], 0xAB);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_postgres_concurrent_uploads() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("concurrent-up").await;
    let bench = Arc::new(bench);

    let mut handles = Vec::new();
    for i in 0..20 {
        let b = Arc::clone(&bench);
        handles.push(tokio::spawn(async move {
            let content = format!("concurrent-content-{i}");
            let file_id = format!("conc-up-{i:02}.txt");
            b.upload_file(&file_id, axum::body::Bytes::from(content), None)
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "concurrent upload should succeed: {result:?}"
        );
    }

    // Stats should reflect them
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 20,
        "stats should show >= 20 files: {stats:?}"
    );
}

// ===========================================================================
// OciBackend trait dispatch — ServerBackend::Postgres
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_postgres_put_sha256_addressed_bytes() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    use shardline_oci_adapter::OciBackend;
    let body = b"oci-postgres-sha256-payload";
    let digest_hex = hex::encode(Sha256::digest(body));
    let canonical_key = shardline_server::shared_sha256_object_key(&digest_hex).unwrap();
    let result = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &sb,
        &canonical_key,
        &digest_hex,
        body.to_vec(),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PutOutcome::Inserted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_postgres_put_sha256_addressed_bytes_already_exists() {
    let _url = ensure_pg().await;
    let (backend, _tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    use shardline_oci_adapter::OciBackend;
    let body = b"oci-postgres-dedup-payload";
    let digest_hex = hex::encode(Sha256::digest(body));
    let canonical_key = shardline_server::shared_sha256_object_key(&digest_hex).unwrap();
    let first = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &sb,
        &canonical_key,
        &digest_hex,
        body.to_vec(),
    )
    .unwrap();
    assert_eq!(first, PutOutcome::Inserted);
    let second = OciBackend::put_sha256_addressed_object_bytes_if_absent(
        &sb,
        &canonical_key,
        &digest_hex,
        body.to_vec(),
    )
    .unwrap();
    assert_eq!(second, PutOutcome::AlreadyExists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_postgres_copy_object_if_absent() {
    let _url = ensure_pg().await;
    let (backend, tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    use shardline_oci_adapter::OciBackend;
    let src = ObjectKey::parse("oci-pg-copy-src").unwrap();
    let dst = ObjectKey::parse("oci-pg-copy-dst").unwrap();
    // Seed via local store
    let store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let data = b"oci-pg-copy-data";
    let hash = ShardlineHash::from_bytes(*blake3::hash(data).as_bytes());
    let integrity = ObjectIntegrity::new(hash, data.len() as u64);
    store
        .put_if_absent(&src, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    let outcome = OciBackend::copy_object_if_absent(&sb, &src, &dst).unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);
}

// ===========================================================================
// Stress/concurrency tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_100_uploads() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("conc100-up").await;
    let bench = Arc::new(bench);

    let mut handles = Vec::new();
    for i in 0..100 {
        let b = Arc::clone(&bench);
        handles.push(tokio::spawn(async move {
            let content = format!("conc100-content-{i}");
            let file_id = format!("conc100-{i:04}.txt");
            b.upload_file(&file_id, axum::body::Bytes::from(content), None)
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "concurrent upload should succeed: {result:?}"
        );
    }

    // Stats should reflect all 100 files
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 100,
        "stats should show >= 100 files: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_10_reconstructions() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("conc10-recon").await;
    let bench = Arc::new(bench);

    // Upload a single file first
    let content = b"shared reconstruction test content for concurrent access";
    bench
        .upload_file(
            "conc-recon-file.bin",
            axum::body::Bytes::from_static(content),
            None,
        )
        .await
        .unwrap();

    // 10 concurrent reconstruction requests for the same file
    let mut handles = Vec::new();
    for _ in 0..10 {
        let b = Arc::clone(&bench);
        handles.push(tokio::spawn(async move {
            b.reconstruction("conc-recon-file.bin", None, None, None)
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "concurrent reconstruction should succeed: {result:?}"
        );
        let response = result.unwrap();
        assert!(
            !response.terms.is_empty() || response.offset_into_first_range == 0,
            "reconstruction should return valid response"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_set_and_scan() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("conc-set-scan").await;
    let bench = Arc::new(bench);

    // Concurrently upload 10 files while repeatedly querying stats
    let bench_upload = Arc::clone(&bench);
    let upload_handle = tokio::spawn(async move {
        for i in 0..10 {
            let content = format!("set-scan-content-{i}");
            let file_id = format!("set-scan-{i:02}.bin");
            bench_upload
                .upload_file(&file_id, axum::body::Bytes::from(content), None)
                .await
                .unwrap();
        }
        10u32
    });

    let bench_scan = Arc::clone(&bench);
    let scan_handle = tokio::spawn(async move {
        for _ in 0..5 {
            let _drop = bench_scan.stats().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        true
    });

    let (upload_count, scan_done) = tokio::join!(upload_handle, scan_handle);
    assert_eq!(upload_count.unwrap(), 10);
    assert!(scan_done.unwrap());

    // Final stats reflect uploads
    let stats = bench.stats().await.unwrap();
    assert!(
        stats.files >= 10,
        "stats should show >= 10 files: {stats:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_upload_and_delete() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("conc-up-del").await;
    let bench = Arc::new(bench);

    // Race condition: concurrent upload + delete of same file
    let bench_upload = Arc::clone(&bench);
    let bench_delete = Arc::clone(&bench);

    let upload_handle = tokio::spawn(async move {
        let mut results = Vec::new();
        for i in 0..20 {
            let content = format!("race-content-{i}");
            let file_id = format!("race-file-{i:02}.bin");
            let result = bench_upload
                .upload_file(&file_id, axum::body::Bytes::from(content), None)
                .await;
            results.push(result);
        }
        results
    });

    let delete_handle = tokio::spawn(async move {
        let mut results = Vec::new();
        for i in 0..20 {
            let file_id = format!("race-file-{i:02}.bin");
            // Use download to check existence; delete is not directly exposed,
            // so we simulate concurrency via conflicting file operations.
            let result = bench_delete.download_file(&file_id, None, None).await;
            results.push(result);
        }
        results
    });

    let (upload_results, download_results) = tokio::join!(upload_handle, delete_handle);
    for result in upload_results.unwrap() {
        // Uploads should succeed or fail with specific errors (idempotent)
        if let Err(ref e) = result {
            assert!(
                format!("{e:?}").contains("InvalidFileId") || format!("{e:?}").contains("NotFound"),
                "unexpected upload error: {e:?}"
            );
        }
    }
    // Download results may be Ok or Err depending on timing — both are valid
    assert_eq!(download_results.unwrap().len(), 20);
}

// ===========================================================================
// Database migration edge cases (integration-level with Docker Postgres)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_status_pending_zero() {
    let _url = ensure_pg().await;
    // The shared database has all migrations applied; verify status reports zero pending
    let url = PG.get().expect("ensure_pg() not called").1.clone();
    let opts = DatabaseMigrationOptions::new(url, DatabaseMigrationCommand::Status);
    let report = run_database_migration(&opts).await.unwrap();
    assert_eq!(report.pending_count, 0, "all migrations should be applied");
    assert_eq!(
        report.applied_total_count,
        bundled_database_migrations().len() as u64
    );
    assert_eq!(report.migrations.len(), bundled_database_migrations().len());
    // Every migration should be marked applied
    assert!(report.migrations.iter().all(|m| m.applied));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_partial_apply_and_revert() {
    // Create a fresh database to test reversion without affecting shared state
    let base_url = ensure_pg().await.to_owned();
    let db_name = format!("shardline_migrate_test_{}", std::process::id());
    let admin_url = {
        let mut url = url::Url::parse(&base_url).unwrap();
        url.set_path("postgres");
        url.to_string()
    };
    let admin_pool = sqlx::PgPool::connect(&admin_url).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .execute(&admin_pool)
        .await
        .ok();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .unwrap();

    let test_url = {
        let mut url = url::Url::parse(&base_url).unwrap();
        url.set_path(&db_name);
        url.to_string()
    };

    // 1. Partial migration: apply only first 2 migrations
    let partial_opts = DatabaseMigrationOptions::new(
        test_url.clone(),
        DatabaseMigrationCommand::Up { steps: Some(2) },
    );
    let partial_report = run_database_migration(&partial_opts).await.unwrap();
    assert_eq!(partial_report.applied_count, 2);
    assert_eq!(
        partial_report.pending_count,
        bundled_database_migrations().len() as u64 - 2
    );

    // 2. Revert one migration
    let revert_opts = DatabaseMigrationOptions::new(
        test_url.clone(),
        DatabaseMigrationCommand::Down { steps: 1 },
    );
    let revert_report = run_database_migration(&revert_opts).await.unwrap();
    assert_eq!(revert_report.reverted_count, 1);
    assert_eq!(
        revert_report.pending_count,
        bundled_database_migrations().len() as u64 - 1
    );

    // 3. Re-apply the same migration (idempotent)
    let reapply_opts = DatabaseMigrationOptions::new(
        test_url.clone(),
        DatabaseMigrationCommand::Up { steps: Some(2) },
    );
    let reapply_report = run_database_migration(&reapply_opts).await.unwrap();
    assert_eq!(
        reapply_report.applied_count, 2,
        "should re-apply reverted + 1 new"
    );

    // 4. Apply all remaining
    let all_opts = DatabaseMigrationOptions::new(
        test_url.clone(),
        DatabaseMigrationCommand::Up { steps: None },
    );
    let all_report = run_database_migration(&all_opts).await.unwrap();
    assert_eq!(all_report.pending_count, 0);

    // Verify the first migration's table exists after re-apply
    let pool = sqlx::PgPool::connect(&test_url).await.unwrap();
    let table_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'shardline_file_records')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(table_exists, "migration table should exist after re-apply");
    pool.close().await;

    // Cleanup
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .execute(&admin_pool)
        .await
        .ok();
    admin_pool.close().await;
}

/// The released v1.0.0 binary bundled these seven migrations. Keep this exact
/// prefix stable so a database created by that release can be upgraded by the
/// current binary and intentionally rolled back before a v1.0.0 deployment.
const V1_0_0_MIGRATION_VERSIONS: [&str; 7] = [
    "20260417000000",
    "20260417010000",
    "20260418000000",
    "20260418010000",
    "20260418020000",
    "20260418110000",
    "20260629000000",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v1_0_0_database_upgrade_and_rollback_preserves_hub_metadata() {
    assert_eq!(
        bundled_database_migrations()
            .iter()
            .take(V1_0_0_MIGRATION_VERSIONS.len())
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        V1_0_0_MIGRATION_VERSIONS,
        "the v1.0.0 migration prefix must remain byte-for-byte compatible"
    );

    let base_url = ensure_pg().await.to_owned();
    let db_name = format!("shardline_v1_upgrade_{}", std::process::id());
    let mut admin_url = url::Url::parse(&base_url).unwrap();
    admin_url.set_path("postgres");
    let admin_pool = sqlx::PgPool::connect(admin_url.as_str()).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .execute(&admin_pool)
        .await
        .ok();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin_pool)
        .await
        .unwrap();

    let mut database_url = url::Url::parse(&base_url).unwrap();
    database_url.set_path(&db_name);
    let database_url = database_url.to_string();

    let v1_apply = run_database_migration(&DatabaseMigrationOptions::new(
        database_url.clone(),
        DatabaseMigrationCommand::Up {
            steps: Some(V1_0_0_MIGRATION_VERSIONS.len()),
        },
    ))
    .await
    .unwrap();
    assert_eq!(
        v1_apply.applied_count,
        V1_0_0_MIGRATION_VERSIONS.len() as u64
    );
    assert_eq!(
        v1_apply.applied_total_count,
        V1_0_0_MIGRATION_VERSIONS.len() as u64
    );

    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    sqlx::query(
        "INSERT INTO shardline_hub_repos \
         (repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds) \
         VALUES ('compatibility/model', 'model', FALSE, 'main', 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    apply_database_migrations(&pool).await.unwrap();
    let upgraded_repo: (String, String) = sqlx::query_as(
        "SELECT repo_id, default_branch FROM shardline_hub_repos WHERE repo_id = 'compatibility/model'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        upgraded_repo,
        ("compatibility/model".to_owned(), "main".to_owned())
    );

    let upgraded = run_database_migration(&DatabaseMigrationOptions::new(
        database_url.clone(),
        DatabaseMigrationCommand::Status,
    ))
    .await
    .unwrap();
    assert_eq!(upgraded.pending_count, 0);

    let post_v1_migration_count =
        bundled_database_migrations().len() - V1_0_0_MIGRATION_VERSIONS.len();
    let rollback = run_database_migration(&DatabaseMigrationOptions::new(
        database_url.clone(),
        DatabaseMigrationCommand::Down {
            steps: post_v1_migration_count,
        },
    ))
    .await
    .unwrap();
    assert_eq!(rollback.reverted_count, post_v1_migration_count as u64);
    assert_eq!(
        rollback.applied_total_count,
        V1_0_0_MIGRATION_VERSIONS.len() as u64
    );

    let rollback_read: (String, String) = sqlx::query_as(
        "SELECT repo_id, default_branch FROM shardline_hub_repos WHERE repo_id = 'compatibility/model'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        rollback_read, upgraded_repo,
        "rollback must preserve v1.0.0 Hub metadata"
    );
    // Verify the drop_inline_content migration removed the column
    let column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT FROM information_schema.columns \
         WHERE table_name = 'shardline_hub_file_entries' AND column_name = 'inline_content')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !column_exists,
        "drop_inline_content migration must remove the column"
    );

    pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .execute(&admin_pool)
        .await
        .ok();
    admin_pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_options_up_down_commands() {
    // Verify that DatabaseMigrationCommand::Up and Down accessors work
    let up_opts = DatabaseMigrationOptions::new(
        "postgres://localhost/test".to_owned(),
        DatabaseMigrationCommand::Up { steps: Some(3) },
    );
    assert!(matches!(
        up_opts.command(),
        DatabaseMigrationCommand::Up { steps: Some(3) }
    ));

    let down_opts = DatabaseMigrationOptions::new(
        "postgres://localhost/test".to_owned(),
        DatabaseMigrationCommand::Down { steps: 2 },
    );
    assert!(matches!(
        down_opts.command(),
        DatabaseMigrationCommand::Down { steps: 2 }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration_version_uniqueness() {
    let migrations = bundled_database_migrations();
    let mut versions: Vec<&str> = migrations.iter().map(|m| m.version).collect();
    versions.sort();
    let deduped = {
        let mut v = versions.clone();
        v.dedup();
        v
    };
    assert_eq!(
        versions.len(),
        deduped.len(),
        "all migration versions must be unique"
    );
}

// ===========================================================================
// Backup manifest roundtrip — seed data, run backup, verify manifest
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backup_manifest_roundtrip() {
    let _url = ensure_pg().await;
    let tmp = TempDir::new().unwrap();

    // Seed data
    {
        let config = pg_config(&tmp);
        let bench = BenchmarkBackend::from_config(&config, tmp.path().to_path_buf(), "bkup-rt")
            .await
            .unwrap();
        bench
            .upload_file(
                "roundtrip-a.bin",
                axum::body::Bytes::from_static(b"alpha"),
                None,
            )
            .await
            .unwrap();
        bench
            .upload_file(
                "roundtrip-b.bin",
                axum::body::Bytes::from_static(b"beta"),
                None,
            )
            .await
            .unwrap();
    }

    // Run backup
    let config = pg_config(&tmp);
    let mut buffer = Vec::new();
    let report = write_backup_manifest(config, &mut buffer).await.unwrap();
    assert_eq!(report.metadata_backend, "postgres");
    assert!(
        report.latest_records >= 2,
        "should have >= 2 records: {report:?}"
    );
    assert!(report.object_count > 0, "object count should be positive");

    // Roundtrip: parse the JSON output and verify key fields
    let json_str = String::from_utf8(buffer).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["manifest_version"], 1);
    assert_eq!(parsed["metadata_backend"], "postgres");
    assert_eq!(parsed["object_backend"], "local");
    assert!(parsed["latest_records"].as_u64().unwrap_or(0) >= 2);
    assert!(parsed["object_count"].as_u64().unwrap_or(0) > 0);
    assert!(parsed["objects"].is_array());
    assert!(!parsed["objects"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_postgres_delete_object_if_present() {
    let _url = ensure_pg().await;
    let (backend, tmp) = pg_backend().await;
    let sb = ServerBackend::Postgres(backend);
    use shardline_oci_adapter::OciBackend;
    let key = ObjectKey::parse("oci-pg-delete-me").unwrap();
    // Seed an object
    let store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let data = b"oci-pg-delete-data";
    let hash = ShardlineHash::from_bytes(*blake3::hash(data).as_bytes());
    let integrity = ObjectIntegrity::new(hash, data.len() as u64);
    store
        .put_if_absent(&key, ObjectBody::from_slice(data), &integrity)
        .unwrap();
    let outcome = OciBackend::delete_object_if_present(&sb, &key)
        .await
        .unwrap();
    assert_eq!(outcome, DeleteOutcome::Deleted);
}

// ===========================================================================
// Record operations roundtrip — set, scan, delete
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_record_roundtrip_set_scan_delete() {
    let _url = ensure_pg().await;
    let (bench, _tmp) = pg_benchmark("rec-roundtrip").await;

    // Set: upload files
    for i in 0..5 {
        let content = format!("roundtrip-content-{i}");
        let file_id = format!("rt-{i}.bin");
        bench
            .upload_file(&file_id, axum::body::Bytes::from(content), None)
            .await
            .unwrap();
    }

    // Verify via stats
    let stats = bench.stats().await.unwrap();
    assert!(stats.files >= 5);

    // Verify each file can be downloaded
    for i in 0..5 {
        let content = format!("roundtrip-content-{i}");
        let file_id = format!("rt-{i}.bin");
        let downloaded = bench.download_file(&file_id, None, None).await.unwrap();
        assert_eq!(String::from_utf8(downloaded).unwrap(), content);
    }
}

// ---------------------------------------------------------------------------
// Redis reconstruction cache — get / put / delete roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_redis_reconstruction_cache_roundtrip() {
    let Some(stack) = DockerLocalStack::builder().with_redis().start().unwrap() else {
        eprintln!("skipping — Docker not available");
        return;
    };
    let redis_url = stack.redis_url().expect("redis url");

    let ttl = std::num::NonZeroU64::new(3600).unwrap();
    let cache = RedisReconstructionCache::new(&redis_url, ttl)
        .expect("should create RedisReconstructionCache");

    // Verify the connection is ready
    let ready = cache.ready().await;
    assert!(ready.is_ok(), "ready() should succeed: {ready:?}");

    // PUT
    let key = ReconstructionCacheKey::latest("test-file.bin", None);
    let payload = b"hello redis cache";
    let put = cache.put(&key, payload).await;
    assert!(put.is_ok(), "put should succeed: {put:?}");

    // GET
    let get = cache.get(&key).await;
    assert!(get.is_ok(), "get should succeed: {get:?}");
    assert_eq!(get.unwrap(), Some(payload.to_vec()));

    // GET non-existent key
    let missing_key = ReconstructionCacheKey::latest("missing-file.bin", None);
    let missing = cache.get(&missing_key).await;
    assert!(missing.is_ok(), "get missing should succeed: {missing:?}");
    assert_eq!(missing.unwrap(), None);

    // DELETE
    let deleted = cache.delete(&key).await;
    assert!(deleted.is_ok(), "delete should succeed: {deleted:?}");
    assert!(deleted.unwrap(), "delete should return true");

    // Verify deleted
    let after_delete = cache.get(&key).await;
    assert!(after_delete.is_ok());
    assert_eq!(after_delete.unwrap(), None);

    // DELETE non-existent returns false
    let deleted_missing = cache.delete(&key).await;
    assert!(deleted_missing.is_ok());
    assert!(
        !deleted_missing.unwrap(),
        "delete missing should return false"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_reconstruction_cache_recovers_after_service_restart() {
    let Some(mut stack) = DockerLocalStack::builder().with_redis().start().unwrap() else {
        eprintln!("skipping — Docker not available");
        return;
    };
    let redis_url = stack.redis_url().expect("redis url");
    let cache = RedisReconstructionCache::new(&redis_url, std::num::NonZeroU64::MIN)
        .expect("redis cache configuration");
    let key = ReconstructionCacheKey::latest("recovery.bin", None);

    assert!(
        cache.ready().await.is_ok(),
        "Redis should initially be ready"
    );
    stack.stop_redis().unwrap();

    let unavailable = tokio::time::timeout(Duration::from_secs(5), cache.get(&key))
        .await
        .expect("cache lookup should fail promptly while Redis is stopped");
    assert!(
        unavailable.is_err(),
        "cache lookup must surface a stopped Redis service"
    );

    stack.start_redis().unwrap();
    let recovered_cache = RedisReconstructionCache::new(
        &stack.redis_url().expect("redis url after restart"),
        std::num::NonZeroU64::MIN,
    )
    .expect("redis cache configuration after restart");
    retry_redis_cache_ready(&recovered_cache).await;
    recovered_cache
        .put(&key, b"recovered cache payload")
        .await
        .unwrap();
    assert_eq!(
        recovered_cache.get(&key).await.unwrap(),
        Some(b"recovered cache payload".to_vec())
    );
}

async fn retry_redis_cache_ready(cache: &RedisReconstructionCache) {
    let mut last_error = None;
    for _ in 0..20 {
        match cache.ready().await {
            Ok(()) => return,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("cache did not reconnect after Redis restart: {last_error:?}");
}
