#![deny(unsafe_code)]

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

#[cfg(not(unix))]
compile_error!(
    "shardline-server requires Unix anchored filesystem semantics; non-Unix builds are unsupported until equivalent TOCTOU hardening exists"
);

mod app;
mod auth;
mod backend;
mod backup;
mod chunk_store;
mod clock;
mod config;
mod database_migration;
mod download_stream;
mod error;
mod fsck;
mod fuzz;
mod gc;
mod ingest_bench;
mod lifecycle_repair;
mod local_backend;
#[cfg(test)]
mod local_fs;
mod local_path;
mod model;
mod object_store;
mod ops_record_store;
mod overflow;
mod postgres_backend;
mod provider;
mod provider_events;
mod rebuild;
mod reconstruction;
mod reconstruction_cache;
mod record_store;
mod repository_scope_path;
mod runtime_check;
mod server_role;
mod shard_store;
mod storage_migration;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod test_invariant_error;
mod transfer_limiter;
mod upload_ingest;
mod validation;
mod xorb_store;
mod xorb_visit;

pub use app::{router, serve, serve_with_listener};
pub use backend::BenchmarkBackend;
pub use backup::{BackupManifestReport, write_backup_manifest};
pub use config::{ObjectStorageAdapter, ServerConfig, ServerConfigError, ShardMetadataLimits};
pub use database_migration::{
    DatabaseMigration, DatabaseMigrationCommand, DatabaseMigrationError, DatabaseMigrationOptions,
    DatabaseMigrationReport, DatabaseMigrationStatusEntry, apply_database_migrations,
    bundled_database_migrations, run_database_migration,
};
pub use error::{
    InvalidLifecycleMetadataError, InvalidReconstructionResponseError, InvalidSerializedShardError,
    ServerError,
};
pub use fsck::{
    FsckIssueDetail, FsckReconstructionPlanDetail, LocalFsckIssue, LocalFsckIssueKind,
    LocalFsckReport, ProviderRepositoryStateTimestampField, run_fsck, run_local_fsck,
};
pub use fuzz::{
    FuzzLifecycleRepairSummary, FuzzReconstructionResponseSummary, FuzzRetainedShardSummary,
    FuzzValidatedXorbSummary, fuzz_lifecycle_repair_summary, fuzz_normalize_and_validate_xorb,
    fuzz_reconstruction_response_summary, fuzz_retained_shard_chunk_hashes,
};
pub use gc::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, GcOrphanInventoryEntry, GcOrphanQuarantineState,
    GcRetentionReportEntry, LocalGcDiagnostics, LocalGcOptions, LocalGcReport, run_gc,
    run_gc_diagnostics, run_local_gc, run_local_gc_diagnostics,
};
pub use ingest_bench::{ingest_without_storage, ingest_without_storage_with_parallelism};
pub use lifecycle_repair::{
    DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, LifecycleRepairOptions, LifecycleRepairReport,
    run_lifecycle_repair, run_local_lifecycle_repair,
};
pub use local_backend::LocalBackend;
pub use model::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    GitLfsAuthenticateResponse, HealthResponse, ProviderTokenIssueRequest,
    ProviderTokenIssueResponse, ProviderWebhookResponse, ReadyResponse, ReconstructionChunkRange,
    ReconstructionFetchInfo, ReconstructionMultiRangeFetch, ReconstructionRangeDescriptor,
    ReconstructionTerm, ReconstructionUrlRange, ServerStatsResponse, ShardUploadResponse,
    UploadChunkResult, UploadFileResponse, XetCasTokenResponse, XorbUploadResponse,
};
pub use postgres_backend::PostgresBackend;
pub use provider_events::{
    ProviderWebhookOutcome, ProviderWebhookOutcomeKind, apply_provider_webhook,
};
pub use rebuild::{
    IndexRebuildIssueDetail, IndexRebuildReconstructionPlanDetail, LocalIndexRebuildIssue,
    LocalIndexRebuildIssueKind, LocalIndexRebuildReport, run_index_rebuild,
    run_local_index_rebuild,
};
pub use reconstruction_cache::{
    ReconstructionCacheBenchReport, benchmark_memory_reconstruction_cache,
};
pub use runtime_check::{ConfigCheckReport, run_config_check};
pub use server_role::{ServerRole, ServerRoleParseError};
pub use storage_migration::{
    StorageMigrationEndpoint, StorageMigrationOptions, StorageMigrationReport,
    run_storage_migration,
};
