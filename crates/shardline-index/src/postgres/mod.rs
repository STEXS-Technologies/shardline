mod index_store;
mod lfs_locks;
mod oci_objects;
mod oci_tags;
mod provider_mutation;
mod record_store;
mod remote_deletes;
mod resumable_sessions;
mod s3_objects;
mod task_artifacts;
mod task_history_scope;
mod tree_store;
mod types;
mod upload_intent_ownership;
mod upload_recovery_guard;

pub(crate) use index_store::{
    insert_reliability_event, load_postgres_latest_evidence_event,
    load_postgres_latest_evidence_heads, next_reliability_sequence,
};
pub use provider_mutation::{
    PostgresProviderMutation, PostgresProviderMutationOutcome, PostgresResourceFence,
    ProviderRepositoryKey,
};
pub use remote_deletes::{PostgresRemoteDeletePermit, RemoteDeletePending};
pub use task_artifacts::PostgresTaskArtifact;
mod remote_delete_receipts;
pub use remote_delete_receipts::{RemoteDeleteReceipt, RemoteDeleteReceiptSettlement};
pub(crate) use resumable_sessions::refresh_resumable_state_digest;
pub(crate) use types::RecordKind;
pub use types::{
    PostgresIndexStore, PostgresMetadataStoreError, PostgresRecordLocator, PostgresRecordStore,
};
pub(super) use types::{i64_to_u64, u64_to_i64};

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use test_support::connect_isolated_postgres;
