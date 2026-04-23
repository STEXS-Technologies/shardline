#![deny(unsafe_code)]

//! Metadata index contracts and adapters for Shardline.
//!
//! This crate models the metadata Shardline needs to reconstruct files from
//! content-addressed xorbs:
//!
//! - [`FileId`] and [`XorbId`] wrap protocol hashes with domain-specific meaning.
//! - [`FileReconstruction`] stores the ordered recipe for one file.
//! - [`IndexStore`] and [`RecordStore`] describe the synchronous metadata
//!   contracts used by local and Postgres-backed deployments.
//! - [`AsyncIndexStore`] exposes the same boundary for async server workflows.
//!
//! # Example
//!
//! ```
//! use shardline_index::{FileId, FileReconstruction, ReconstructionTerm, XorbId};
//! use shardline_protocol::{ChunkRange, ShardlineHash};
//!
//! let file_id = FileId::new(ShardlineHash::from_bytes([1; 32]));
//! let xorb_id = XorbId::new(ShardlineHash::from_bytes([2; 32]));
//! let term = ReconstructionTerm::new(xorb_id, ChunkRange::new(0, 2)?, 128);
//! let reconstruction = FileReconstruction::new(vec![term]);
//!
//! assert_eq!(file_id.hash().as_bytes(), &[1; 32]);
//! assert_eq!(reconstruction.terms()[0].xorb_id(), xorb_id);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod dedupe;
mod ids;
mod lifecycle;
mod local_sqlite;
mod memory;
mod postgres;
mod provider;
mod reconstruction;
mod record;
mod record_key;
mod store;
#[cfg(test)]
mod test_invariant_error;

pub use dedupe::DedupeShardMapping;
pub use ids::{FileId, XorbId};
pub use lifecycle::{
    ProviderRepositoryState, QuarantineCandidate, QuarantineCandidateError, RetentionHold,
    RetentionHoldError, WebhookDelivery, WebhookDeliveryError,
};
pub use local_sqlite::{
    LocalIndexStore, LocalIndexStoreError, LocalRecordLocator, LocalRecordStore,
};
pub use memory::{
    MemoryIndexStore, MemoryIndexStoreError, MemoryRecordLocator, MemoryRecordStore,
    MemoryRecordStoreError,
};
pub use postgres::{
    PostgresIndexStore, PostgresMetadataStoreError, PostgresRecordLocator, PostgresRecordStore,
};
pub use reconstruction::{FileReconstruction, ReconstructionTerm};
pub use record::{
    FileChunkRecord, FileRecord, FileRecordInvariantError, RecordStore, RecordStoreFuture,
    RepositoryRecordScope, StoredRecord,
};
pub use store::{AsyncIndexStore, IndexStore, IndexStoreFuture};
