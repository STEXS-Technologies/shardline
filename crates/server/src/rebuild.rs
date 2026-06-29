use std::path::PathBuf;

use shardline_index::{LocalIndexStore, PostgresIndexStore, PostgresRecordStore};
pub use shardline_rebuild::{
    IndexRebuildIssueDetail, IndexRebuildReconstructionPlanDetail, IndexRebuildReport,
    LocalIndexRebuildIssue, LocalIndexRebuildIssueKind, LocalIndexRebuildReport, RebuildError,
    run_index_rebuild_with_stores,
};
use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;

use crate::{
    ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
};

impl From<RebuildError> for ServerError {
    fn from(value: RebuildError) -> Self {
        match value {
            RebuildError::Io(e) => Self::Io(e),
            RebuildError::Json(e) => Self::Json(e),
            RebuildError::NumericConversion(e) => Self::NumericConversion(e),
            RebuildError::InvalidContentHash => Self::InvalidContentHash,
            RebuildError::InvalidFileId => Self::InvalidFileId,
            RebuildError::Overflow => Self::Overflow,
            RebuildError::ObjectPrefix(e) => Self::ObjectPrefix(e),
            RebuildError::LocalObjectStore(e) => Self::ObjectStore(e),
            RebuildError::S3ObjectStore(e) => Self::S3ObjectStore(e),
            RebuildError::XetAdapter(e) => Self::from(e),
            RebuildError::IndexStore(e) => Self::IndexStore(e),
            RebuildError::MemoryIndexStore(e) => Self::MemoryIndexStore(e),
            RebuildError::MemoryRecordStore(e) => Self::MemoryRecordStore(e),
            RebuildError::PostgresMetadata(e) => Self::PostgresMetadata(e),
            RebuildError::HashParse(e) => Self::HashParse(e),
            RebuildError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        }
    }
}

/// Rebuilds latest-record state against the configured metadata backend.
///
/// # Errors
///
/// Returns [`ServerError`] when version records cannot be scanned or latest records
/// cannot be written or removed.
pub async fn run_index_rebuild(config: ServerConfig) -> Result<IndexRebuildReport, ServerError> {
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        return run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            config.shard_metadata_limits(),
        )
        .await
        .map_err(Into::into);
    }

    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        config.shard_metadata_limits(),
    )
    .await
    .map_err(Into::into)
}

/// Rebuilds local latest-record state from immutable version records.
///
/// The local metadata backend stores immutable version rows and derives visible latest
/// rows in the same record store.
///
/// # Errors
///
/// Returns [`ServerError`] when the storage root cannot be traversed or when latest
/// records cannot be written or removed.
pub async fn run_local_index_rebuild(
    root: PathBuf,
) -> Result<LocalIndexRebuildReport, ServerError> {
    let object_store = ServerObjectStore::local(root.join("chunks"))?;
    let index_store = LocalIndexStore::open(root.clone());
    let record_store = LocalRecordStore::open(root);
    run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests;
