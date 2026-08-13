#![deny(unsafe_code)]

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
//! # Quick start
//!
//! Create an upload intent and follow its state machine, then validate the
//! reconstruction plan of a stored [`FileRecord`]:
//!
//! ```
//! use shardline_index::{
//!     FileChunkRecord, FileRecord, StorageRepresentation, UploadIntent, UploadIntentState,
//! };
//!
//! // A durable upload intent starts in the `Created` state and can only move
//! // forward through the persistence boundaries.
//! let intent = UploadIntent::new(
//!     "intent-1".to_owned(),
//!     "chunks/aa/bb/example.xorb".to_owned(),
//!     "a".repeat(64),
//!     1024,
//! );
//! assert_eq!(intent.state(), UploadIntentState::Created);
//! assert!(intent.state().can_transition_to(UploadIntentState::Storing));
//! assert!(!intent.state().can_transition_to(UploadIntentState::Visible));
//!
//! // A stored file record must describe a contiguous, ordered chunk plan.
//! let hash = "a".repeat(64);
//! let record = FileRecord {
//!     file_id: "assets/logo.png".to_owned(),
//!     content_hash: hash.clone(),
//!     total_bytes: 1024,
//!     chunk_size: 1024,
//!     storage_repr: StorageRepresentation::FixedChunkV1,
//!     repository_scope: None,
//!     chunks: vec![FileChunkRecord {
//!         hash,
//!         offset: 0,
//!         length: 1024,
//!         range_start: 0,
//!         range_end: 1,
//!         packed_start: 0,
//!         packed_end: 1,
//!     }],
//! };
//! record.validate_reconstruction_plan()?;
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
mod record_kind;
#[cfg(test)]
mod test_invariant_error;
mod tree;
mod upload_intent;
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
    RecordStore, RecordStoreFuture, RecordTraversal, RepositoryRecordScope, StorageRepresentation,
    StoredRecord,
};
pub use store::{
    AsyncIndexStore, DedupeStore, IndexStore, IndexStoreFuture, LifecycleStore,
    ReconstructionStore, Repository,
};
pub use tree::{RepoKey, RevisionRecord, TreeEntry, TreeEntryOutcome, TreeKey, TreeStore};
pub use upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore};
pub use xet_hash::{parse_xet_hash_hex, xet_hash_hex_string};
