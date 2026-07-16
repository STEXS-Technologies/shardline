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
        use crate::error::{IndexError, ObjectStoreError};
        match value {
            RebuildError::Io(e) => Self::Io(e),
            RebuildError::Json(e) => Self::Json(e),
            RebuildError::NumericConversion(e) => Self::NumericConversion(e),
            RebuildError::InvalidContentHash => Self::InvalidContentHash,
            RebuildError::InvalidFileId => Self::InvalidFileId,
            RebuildError::Overflow => Self::Overflow,
            RebuildError::ObjectPrefix(e) => Self::ObjectStore(ObjectStoreError::Prefix(e)),
            RebuildError::LocalObjectStore(e) => Self::ObjectStore(ObjectStoreError::Local(e)),
            RebuildError::S3ObjectStore(e) => Self::ObjectStore(ObjectStoreError::S3(e)),
            RebuildError::XetAdapter(e) => Self::from(e),
            RebuildError::IndexStore(e) => Self::Index(IndexError::Local(e)),
            RebuildError::MemoryIndexStore(e) => Self::Index(IndexError::MemoryIndex(e)),
            RebuildError::MemoryRecordStore(e) => Self::Index(IndexError::MemoryRecord(e)),
            RebuildError::PostgresMetadata(e) => Self::Index(IndexError::PostgresMetadata(e)),
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

/// Tests for the `From<RebuildError> for ServerError` conversion.
///
/// These are pure unit tests that can live in the main module without
/// the integration-test infrastructure required by `rebuild/tests.rs`.
#[cfg(test)]
mod from_impl_tests {
    use super::*;
    use crate::error::{IndexError, ObjectStoreError};

    #[test]
    fn from_rebuild_error_io() {
        let err = RebuildError::Io(std::io::Error::other("io"));
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::Io(_)));
    }

    #[test]
    fn from_rebuild_error_json() {
        let json_err = serde_json::from_str::<()>("").unwrap_err();
        let err = RebuildError::Json(json_err);
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::Json(_)));
    }

    #[test]
    fn from_rebuild_error_numeric_conversion() {
        let num_err = u64::try_from(-1i32).unwrap_err();
        let err = RebuildError::NumericConversion(num_err);
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::NumericConversion(_)));
    }

    #[test]
    fn from_rebuild_error_invalid_content_hash() {
        let err = RebuildError::InvalidContentHash;
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::InvalidContentHash));
    }

    #[test]
    fn from_rebuild_error_invalid_file_id() {
        let err = RebuildError::InvalidFileId;
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::InvalidFileId));
    }

    #[test]
    fn from_rebuild_error_overflow() {
        let err = RebuildError::Overflow;
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::Overflow));
    }

    #[test]
    fn from_rebuild_error_object_prefix() {
        let prefix_err = shardline_storage::ObjectPrefixError::UnsafePath;
        let err = RebuildError::ObjectPrefix(prefix_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::ObjectStore(ObjectStoreError::Prefix(_))
        ));
    }

    #[test]
    fn from_rebuild_error_local_object_store() {
        let local_err =
            shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("local"));
        let err = RebuildError::LocalObjectStore(local_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::ObjectStore(ObjectStoreError::Local(_))
        ));
    }

    #[test]
    fn from_rebuild_error_s3_object_store() {
        let s3_err = shardline_storage::S3ObjectStoreError::IncompleteCredentials;
        let err = RebuildError::S3ObjectStore(s3_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::ObjectStore(ObjectStoreError::S3(_))
        ));
    }

    #[test]
    fn from_rebuild_error_xet_adapter() {
        let xet_err = shardline_xet_adapter::XetAdapterError::Overflow;
        let err = RebuildError::XetAdapter(xet_err);
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::Overflow));
    }

    #[test]
    fn from_rebuild_error_index_store() {
        let io_err = std::io::Error::other("index io");
        let index_err = shardline_index::LocalIndexStoreError::Io(io_err);
        let err = RebuildError::IndexStore(index_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::Index(IndexError::Local(_))
        ));
    }

    #[test]
    fn from_rebuild_error_memory_index_store() {
        let mem_err = shardline_index::MemoryIndexStoreError::LockPoisoned;
        let err = RebuildError::MemoryIndexStore(mem_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::Index(IndexError::MemoryIndex(_))
        ));
    }

    #[test]
    fn from_rebuild_error_memory_record_store() {
        let mem_err = shardline_index::MemoryRecordStoreError::LockPoisoned;
        let err = RebuildError::MemoryRecordStore(mem_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::Index(IndexError::MemoryRecord(_))
        ));
    }

    #[test]
    fn from_rebuild_error_postgres_metadata() {
        let pg_err = shardline_index::PostgresMetadataStoreError::HashParse(
            shardline_protocol::HashParseError::InvalidLength,
        );
        let err = RebuildError::PostgresMetadata(pg_err);
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn from_rebuild_error_hash_parse() {
        let hash_err = shardline_protocol::HashParseError::InvalidLength;
        let err = RebuildError::HashParse(hash_err);
        let server_err: ServerError = err.into();
        assert!(matches!(server_err, ServerError::HashParse(_)));
    }

    #[test]
    fn from_rebuild_error_stored_file_metadata_too_large() {
        let err = RebuildError::StoredFileMetadataTooLarge {
            observed_bytes: 5000,
            maximum_bytes: 1000,
        };
        let server_err: ServerError = err.into();
        assert!(matches!(
            server_err,
            ServerError::StoredFileMetadataTooLarge {
                observed_bytes: 5000,
                maximum_bytes: 1000,
            }
        ));
    }
}

#[cfg(test)]
mod tests;
