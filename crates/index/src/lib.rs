#![deny(unsafe_code)]
#![allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        unused_imports,
        unused_variables,
        unused_mut,
        dead_code,
    )
)]

//! Metadata index contracts and adapters for Shardline.
//!
//! This crate models the metadata Shardline needs to reconstruct files from
//! immutable content-addressed objects:
//!
//! - [`FileId`] and [`StoredObjectId`] wrap protocol hashes with domain-specific meaning.
//! - [`FileReconstruction`] stores the ordered recipe for one file.
//! - [`IndexStore`] and [`RecordStore`] describe the synchronous metadata
//!   contracts used by local and Postgres-backed deployments.
//! - [`AsyncIndexStore`] exposes the same boundary for async server workflows.
//!
//! # Example
//!
//! ```
//! use shardline_index::{FileId, FileReconstruction, ReconstructionTerm, StoredObjectId};
//! use shardline_protocol::{ChunkRange, ShardlineHash};
//!
//! let file_id = FileId::new(ShardlineHash::from_bytes([1; 32]));
//! let object_id = StoredObjectId::new(ShardlineHash::from_bytes([2; 32]));
//! let term = ReconstructionTerm::new(object_id, ChunkRange::new(0, 2)?, 128);
//! let reconstruction = FileReconstruction::new(vec![term]);
//!
//! assert_eq!(file_id.hash().as_bytes(), &[1; 32]);
//! assert_eq!(reconstruction.terms()[0].object_id(), object_id);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#[macro_use]
mod store;

mod dedupe;
pub mod hub;
mod hub_local_sqlite;
mod hub_postgres;
mod ids;
mod lifecycle;
mod local_sqlite;
mod memory;
mod postgres;
mod provider;
mod reconstruction;
mod record;
mod record_key;
#[cfg(test)]
mod test_invariant_error;
mod xet_hash;

pub use dedupe::DedupeShardMapping;
pub use ids::{FileId, StoredObjectId, XorbId};
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
    FileChunkRecord, FileRecord, FileRecordInvariantError, FileRecordStorageLayout, RecordMutation,
    RecordStore, RecordStoreFuture, RecordTraversal, RepositoryRecordScope, StoredRecord,
};
pub use store::{
    AsyncIndexStore, DedupeStore, IndexStore, IndexStoreFuture, LifecycleStore,
    ReconstructionStore, Repository,
};
pub use xet_hash::{parse_xet_hash_hex, xet_hash_hex_string};
