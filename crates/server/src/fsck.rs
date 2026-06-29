use shardline_fsck::run_fsck_with_stores;
use shardline_index::{LocalIndexStore, LocalRecordStore, PostgresIndexStore, PostgresRecordStore};

use crate::{
    ServerConfig, ServerError, object_store::object_store_from_config,
    postgres_backend::connect_postgres_metadata_pool,
};

pub use shardline_fsck::{
    FsckError, FsckIssueDetail, FsckIssueKind, FsckReconstructionPlanDetail, LocalFsckIssue,
    LocalFsckIssueKind, LocalFsckReport, ProviderRepositoryStateTimestampField,
    WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS, run_local_fsck,
};

/// Runs integrity checks against the configured metadata backend and local chunk storage.
///
/// # Errors
///
/// Returns [`ServerError`] when the storage root cannot be traversed, metadata cannot be
/// queried, or chunk/record bytes cannot be read.
pub async fn run_fsck(config: ServerConfig) -> Result<LocalFsckReport, ServerError> {
    let object_root = config.root_dir().join("chunks");
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        return run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            config.shard_metadata_limits(),
        )
        .await
        .map_err(ServerError::from);
    }

    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        config.shard_metadata_limits(),
    )
    .await
    .map_err(ServerError::from)
}

impl From<FsckError> for ServerError {
    fn from(value: FsckError) -> Self {
        match value {
            FsckError::Io(e) => Self::Io(e),
            FsckError::Json(e) => Self::Json(e),
            FsckError::NumericConversion(e) => Self::NumericConversion(e),
            FsckError::Overflow => Self::Overflow,
            FsckError::LocalObjectStore(e) => Self::ObjectStore(e),
            FsckError::S3ObjectStore(e) => Self::S3ObjectStore(e),
            FsckError::ObjectStore(e) => match e {
                shardline_server_core::ServerObjectStoreError::NotFound => Self::NotFound,
                shardline_server_core::ServerObjectStoreError::Overflow => Self::Overflow,
                shardline_server_core::ServerObjectStoreError::InvalidContentHash => {
                    Self::InvalidContentHash
                }
                shardline_server_core::ServerObjectStoreError::StoredObjectLengthMismatch => {
                    Self::StoredObjectLengthMismatch
                }
                shardline_server_core::ServerObjectStoreError::Local(e) => Self::ObjectStore(e),
                shardline_server_core::ServerObjectStoreError::S3(e) => Self::S3ObjectStore(e),
                shardline_server_core::ServerObjectStoreError::Io(e) => Self::Io(e),
                shardline_server_core::ServerObjectStoreError::NumericConversion(e) => {
                    Self::NumericConversion(e)
                }
            },
            FsckError::XetAdapter(e) => Self::from(e),
            FsckError::LocalIndexStore(e) => Self::IndexStore(e),
            FsckError::MemoryIndexStore(e) => Self::MemoryIndexStore(e),
            FsckError::MemoryRecordStore(e) => Self::MemoryRecordStore(e),
            FsckError::PostgresMetadata(e) => Self::PostgresMetadata(e),
            FsckError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        }
    }
}
