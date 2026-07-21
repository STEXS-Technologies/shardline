mod async_index_store;
mod error;
mod helpers;
mod index_store;
mod migration;
mod record_store;
mod records;
mod store;
#[cfg(test)]
mod tests;

pub use error::LocalIndexStoreError;
pub(crate) use helpers::{collect_rows, i64_to_u64, record_not_found_error, u64_to_i64};
pub(crate) use migration::LOCAL_SQLITE_MIGRATIONS;
pub(crate) use records::{
    DedupeShardRecord, FileReconstructionRecord, LegacyQuarantineCandidateRecord,
    StoredObjectPresenceRecord,
};
pub(crate) use store::LocalRecordKind;
pub use store::{LocalIndexStore, LocalRecordLocator, LocalRecordStore};

pub(crate) const LOCAL_METADATA_DATABASE_FILE_NAME: &str = "metadata.sqlite3";
const LOCAL_SCHEMA_MIGRATIONS_TABLE: &str = "shardline_local_schema_migrations";
const LEGACY_IMPORT_COMPLETED_KEY: &str = "legacy_filesystem_import_completed";
const MAX_CONTROL_PLANE_METADATA_BYTES: u64 = 1_048_576;
const MAX_RECONSTRUCTION_METADATA_BYTES: u64 = 1_073_741_824;
const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;
