use shardline_index::{
    FileRecordInvariantError, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError, QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError,
};
use shardline_server_core::{InvalidLifecycleMetadataError, InvalidReconstructionResponseError};
use thiserror::Error;

/// Metadata index subsystem failure.
#[derive(Debug, Error)]
pub enum IndexError {
    /// Index adapter access failed.
    #[error("index adapter operation failed")]
    Local(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndex(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecord(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Retention hold input was invalid.
    #[error("retention hold input was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// Quarantine candidate input was invalid.
    #[error("quarantine candidate input was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// Webhook delivery metadata was invalid.
    #[error("webhook delivery metadata was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// Stored file metadata could not produce a valid reconstruction plan.
    #[error("stored file metadata was invalid")]
    FileRecordInvariant(#[from] FileRecordInvariantError),
    /// Lifecycle metadata was internally inconsistent.
    #[error("lifecycle metadata was internally inconsistent")]
    InvalidLifecycleMetadata(#[from] InvalidLifecycleMetadataError),
    /// A reconstruction response violated an internal protocol-shape invariant.
    #[error("reconstruction response invariant failed: {0}")]
    InvalidReconstructionResponse(#[from] InvalidReconstructionResponseError),
    /// A required metadata table was missing.
    #[error("required metadata table is missing: {0}")]
    MissingRequiredMetadataTable(String),
    /// Repository rename encountered conflicting target-scope metadata.
    #[error("repository rename target already contains conflicting metadata")]
    ConflictingRenameTargetRecord,
}
