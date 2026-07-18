#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::shadow_unrelated,
        clippy::format_push_string,
        clippy::let_underscore_must_use
    )
)]

//! HTTP server runtime and operator workflows for Shardline.
//!
//! This crate owns the deployable server boundary: HTTP routing, configuration,
//! local and Postgres metadata backends, provider token issuance, integrity
//! checks, repair flows, garbage collection, backup manifests, and storage
//! migration.
//!
//! Most embedders only need [`ServerConfig`] and [`serve`]. Operational tooling
//! can use the same library entry points exposed by the `shardline` CLI, such as
//! [`run_fsck`], [`run_lifecycle_repair`], [`run_gc`], and
//! [`run_storage_migration`].
//!
//! # Example
//!
//! ```no_run
//! use shardline_server::{ServerConfig, serve};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ServerConfig::from_env()?;
//!     serve(config).await?;
//!     Ok(())
//! }
//! ```

pub mod app;
mod auth;
mod backend;
mod backup;
mod chunk_store;
mod clock;
mod config;
mod database_migration;
pub mod download_stream;
mod error;
mod fsck;
#[cfg(feature = "fuzzing")]
mod fuzz;
mod ingest_bench;
mod jwks_provider;
mod lifecycle_repair;
mod local_backend;
mod local_fs;
mod local_path;
pub mod metrics;
mod model;
mod object_store;
mod oidc_provider;
mod ops_record_store;
mod overflow;
mod postgres_backend;
mod protocol_support;
mod provider;
mod provider_events;
mod rebuild;
mod reconstruction_cache;
mod record_store;
mod repository_scope_path;
mod runtime_check;
mod server_frontend;
mod server_role;
mod storage_migration;
#[cfg(test)]
pub mod test_fixtures;
#[cfg(test)]
pub mod test_invariant_error;

pub use app::ProtocolMetrics;
pub use app::{
    AppState, MAX_PROVIDER_NAME_BYTES, MAX_PROVIDER_SUBJECT_BYTES,
    MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES, MAX_PROVIDER_WEBHOOK_BODY_BYTES,
    acquire_chunk_transfer_permit, full_byte_stream_response,
};
pub use backend::{BenchmarkBackend, ServerBackend};
pub use download_stream::{STREAM_READ_BUFFER_BYTES, ServerByteStream};
pub use local_backend::chunk_hash;
pub use object_store::ServerObjectStore;
pub use oci_adapter::{oci_blob_key, oci_manifest_key, oci_manifest_media_type_key};
pub use protocol_support::shared_sha256_object_key;
pub use reconstruction_cache::ReconstructionCacheService;
pub(crate) mod oci_adapter {
    pub(crate) use shardline_oci_adapter::{
        OciReference, abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes,
        append_upload_bytes, create_upload_session, delete_upload_session,
        finalize_s3_multipart_upload_session, lock_upload_sessions, oci_blob_location,
        oci_manifest_location, oci_manifest_prefix, oci_tag_key, oci_tag_prefix,
        oci_tag_target_key, oci_tag_target_prefix, parse_reference, read_upload_session,
        touch_upload_session, upload_body_integrity, upload_body_path_for_session, upload_length,
        upload_session_length, upload_session_location, validate_repository,
    };
    pub use shardline_oci_adapter::{oci_blob_key, oci_manifest_key, oci_manifest_media_type_key};
}
pub use shardline_protocol_adapters::{BazelCacheKind, bazel_cache_object_key, lfs_object_key};
pub use transfer_limiter::TransferLimiter;
#[cfg(test)]
mod gc_tests;
mod transfer_limiter;
mod upload_ingest;
mod validation;
pub(crate) mod xet_adapter {
    pub use shardline_xet_adapter::{
        BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
        XorbUploadResponse, decode_serialized_xorb_chunks, try_for_each_serialized_xorb_chunk,
        validate_serialized_xorb,
    };
    #[cfg(test)]
    pub(crate) use shardline_xet_adapter::{
        ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionTerm,
        ReconstructionUrlRange, shard_object_key, store_uploaded_xorb,
    };
    pub(crate) use shardline_xet_adapter::{
        ShardUploadResponse, XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE, XORB_TRANSFER_ROUTE,
        XetAdapterError, XorbParseError, XorbVisitError, build_batch_reconstruction_response,
        build_reconstruction_response, reconstruction_v2_from_v1, register_uploaded_shard_bytes,
        resolve_dedupe_shard_object, shard_hash_from_object_key_if_present,
        store_uploaded_xorb_bytes, validate_hash_path, validate_optional_content_hash,
        validate_xorb_transfer_namespace, visit_stored_xorb_chunk_hashes,
        xorb_hash_from_object_key_if_present, xorb_object_key,
    };
    #[cfg(feature = "fuzzing")]
    pub(crate) use shardline_xet_adapter::{
        build_xorb_transfer_url, normalize_serialized_xorb, retained_shard_chunk_hashes,
    };
}

pub use app::{serve, serve_with_listener};
pub use backup::{BackupManifestReport, write_backup_manifest};
pub use config::{
    AuthProviderKind, ObjectStorageAdapter, ServerConfig, ServerConfigError, ShardMetadataLimits,
};
pub use database_migration::{
    DatabaseMigration, DatabaseMigrationCommand, DatabaseMigrationError, DatabaseMigrationOptions,
    DatabaseMigrationReport, DatabaseMigrationStatusEntry, apply_database_migrations,
    bundled_database_migrations, run_database_migration,
};
#[cfg(feature = "fuzzing")]
pub(crate) use error::InvalidReconstructionResponseError;
pub(crate) use error::InvalidSerializedShardError;
pub use error::{ObjectStoreError, ServerError};
pub use fsck::{
    FsckIssueDetail, FsckIssueKind, FsckReconstructionPlanDetail, LocalFsckIssue,
    LocalFsckIssueKind, LocalFsckReport, ProviderRepositoryStateTimestampField, run_fsck,
    run_local_fsck,
};
#[doc(hidden)]
#[cfg(feature = "fuzzing")]
pub use fuzz::{
    FuzzBazelHttpFrontendSummary, FuzzLfsFrontendSummary, FuzzLifecycleRepairSummary,
    FuzzOciFrontendSummary, FuzzProtocolFrontendSummary, FuzzQuarantineAction,
    FuzzReconstructionResponseSummary, FuzzRetainedShardSummary, FuzzRetentionAction,
    FuzzValidatedXorbSummary, FuzzWebhookAction, fuzz_bazel_http_frontend_summary,
    fuzz_classify_quarantine, fuzz_classify_retention, fuzz_classify_webhook,
    fuzz_lfs_frontend_summary, fuzz_lifecycle_repair_summary, fuzz_normalize_and_validate_xorb,
    fuzz_oci_frontend_summary, fuzz_protocol_frontend_summary,
    fuzz_reconstruction_response_summary, fuzz_retained_shard_chunk_hashes,
};
pub use gc::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, LocalGcDiagnostics, LocalGcOptions, LocalGcReport,
};
pub use ingest_bench::ingest_without_storage_with_parallelism;
pub use lifecycle_repair::{
    DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, LifecycleRepairOptions, LifecycleRepairReport,
    run_lifecycle_repair, run_local_lifecycle_repair,
};
pub use local_backend::LocalBackend;
pub use model::{
    GitLfsAuthenticateResponse, HealthResponse, ProviderTokenIssueRequest,
    ProviderTokenIssueResponse, ProviderWebhookResponse, ReadyResponse, ServerStatsResponse,
    XetCasTokenResponse,
};
pub use postgres_backend::PostgresBackend;
pub use rebuild::{
    IndexRebuildIssueDetail, IndexRebuildReconstructionPlanDetail, LocalIndexRebuildIssue,
    LocalIndexRebuildIssueKind, LocalIndexRebuildReport, run_index_rebuild,
    run_local_index_rebuild,
};
pub use reconstruction_cache::{
    ReconstructionCacheBenchReport, benchmark_memory_reconstruction_cache,
};
pub use runtime_check::{ConfigCheckReport, run_config_check};
pub use server_frontend::{ServerFrontend, ServerFrontendParseError};
pub use server_role::{ServerRole, ServerRoleParseError};
pub(crate) mod gc {
    pub(crate) use shardline_gc::run_gc_with_stores;
    pub use shardline_gc::{
        DEFAULT_LOCAL_GC_RETENTION_SECONDS, LocalGcDiagnostics, LocalGcOptions, LocalGcReport,
    };
    #[cfg(test)]
    pub(crate) use shardline_gc::{
        GcError, GcOrphanQuarantineState, quarantine_record_path, quarantine_root, run_local_gc,
        run_local_gc_diagnostics,
    };
}
pub(crate) use shardline_protocol_adapters::{
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectResponse,
};
pub(crate) use shardline_xet_adapter::ShardUploadResponse;
pub use storage_migration::{
    StorageMigrationEndpoint, StorageMigrationOptions, StorageMigrationReport,
    run_storage_migration,
};
pub use xet_adapter::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    XorbUploadResponse, decode_serialized_xorb_chunks, try_for_each_serialized_xorb_chunk,
    validate_serialized_xorb,
};

use object_store::object_store_from_config;
use postgres_backend::connect_postgres_metadata_pool;
use shardline_index::{LocalIndexStore, LocalRecordStore, PostgresIndexStore, PostgresRecordStore};

/// Runs garbage collection against the configured metadata backend and local chunk storage.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc(
    config: ServerConfig,
    options: gc::LocalGcOptions,
) -> Result<gc::LocalGcReport, error::ServerError> {
    Ok(run_gc_diagnostics(config, options).await?.report)
}

/// Runs garbage collection and returns operator diagnostics.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc_diagnostics(
    config: ServerConfig,
    options: gc::LocalGcOptions,
) -> Result<gc::LocalGcDiagnostics, error::ServerError> {
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        return gc::run_gc_with_stores(
            &record_store,
            &index_store,
            &object_store,
            config.server_frontends(),
            options,
        )
        .await
        .map_err(Into::into);
    }

    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    gc::run_gc_with_stores(
        &record_store,
        &index_store,
        &object_store,
        config.server_frontends(),
        options,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod lib_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_gc_succeeds_on_valid_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::ServerConfig::new(
            std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            tmp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let options = super::gc::LocalGcOptions::default();
        let result = run_gc(config, options).await;
        // On a valid temp directory with default options (mark=false, sweep=false),
        // GC should succeed as a no-op
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_gc_diagnostics_succeeds_on_valid_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::ServerConfig::new(
            std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            tmp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let options = super::gc::LocalGcOptions::default();
        let result = run_gc_diagnostics(config, options).await;
        assert!(result.is_ok());
    }
}
