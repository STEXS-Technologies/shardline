#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::items_after_test_module
    )
)]

//! Provider webhook event processing for the Shardline server.
//!
//! This crate handles incoming provider webhook events (repository deleted,
//! renamed, access changed, revision pushed) and applies the corresponding
//! metadata mutations.

use std::num::TryFromIntError;

use serde_json::Error as JsonError;
use shardline_index::{
    AsyncIndexStore, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError, RecordStore, RetentionHoldError, WebhookDelivery,
    WebhookDeliveryError,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server_core::{ParseStoredFileRecordError, ServerObjectStoreError};
use shardline_vcs::{ProviderKind, RepositoryWebhookEvent, RepositoryWebhookEventKind};
use shardline_xet_adapter::XetAdapterError;
use thiserror::Error;

mod outcome;
mod records;
mod repository;
mod state;
#[cfg(test)]
mod tests;

/// Summary of one handled provider webhook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderWebhookOutcome {
    /// Affected provider family.
    pub provider: ProviderKind,
    /// Affected repository owner or namespace.
    pub owner: String,
    /// Affected repository name.
    pub repo: String,
    /// Provider delivery identifier.
    pub delivery_id: String,
    /// Normalized webhook event kind.
    pub event_kind: ProviderWebhookOutcomeKind,
    /// Number of file-version records scanned for the repository.
    pub affected_file_versions: u64,
    /// Number of distinct chunk objects referenced by affected file versions.
    pub affected_chunks: u64,
    /// Number of retention holds inserted or refreshed by the event.
    pub applied_holds: u64,
    /// Retention applied to newly created holds, if any.
    pub retention_seconds: Option<u64>,
}

/// Publicly reportable webhook outcome kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderWebhookOutcomeKind {
    /// Repository deletion created durable retention holds.
    RepositoryDeleted,
    /// Repository rename migrated durable metadata into the new repository scope.
    RepositoryRenamed {
        /// New repository owner or namespace.
        new_owner: String,
        /// New repository name.
        new_repo: String,
    },
    /// Repository access changed without durable state mutation.
    AccessChanged,
    /// A revision moved without durable state mutation.
    RevisionPushed {
        /// Updated revision reference.
        revision: String,
    },
}

/// Default retention window for new local quarantine candidates.
pub use shardline_server_core::DEFAULT_LOCAL_GC_RETENTION_SECONDS;

/// Provider webhook event processing failure.
#[derive(Debug, Error)]
pub enum ProviderEventsError {
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// The provider webhook payload was invalid.
    #[error("provider webhook payload was invalid")]
    InvalidProviderWebhookPayload,
    /// Repository rename encountered conflicting target-scope metadata.
    #[error("repository rename target already contains conflicting metadata")]
    ConflictingRenameTargetRecord,
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] JsonError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Retention hold input was invalid.
    #[error("retention hold input was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// Xet adapter operation failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
    /// Index adapter access failed.
    #[error("index adapter operation failed")]
    IndexStore(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndexStore(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecordStore(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Webhook delivery metadata was invalid.
    #[error("webhook delivery metadata was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// Object-store backend operation failed.
    #[error("object storage operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// Stored file record parsing failed.
    #[error("stored file record parsing failed")]
    ParseStoredFileRecord(#[from] ParseStoredFileRecordError),
}

#[cfg(test)]
mod lib_tests {
    use shardline_vcs::{
        ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind,
        RevisionRef, WebhookDeliveryId,
    };

    use super::ProviderWebhookOutcomeKind;

    #[test]
    fn duplicate_webhook_outcome_for_deleted() {
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap(),
            WebhookDeliveryId::new("delivery-dup").unwrap(),
            RepositoryWebhookEventKind::RepositoryDeleted,
        );
        let outcome = super::duplicate_webhook_outcome(&event);
        assert_eq!(outcome.provider, ProviderKind::GitHub);
        assert_eq!(outcome.owner, "org");
        assert_eq!(outcome.repo, "repo");
        assert_eq!(
            outcome.event_kind,
            ProviderWebhookOutcomeKind::RepositoryDeleted
        );
        assert_eq!(outcome.affected_file_versions, 0);
        assert_eq!(outcome.affected_chunks, 0);
        assert_eq!(outcome.applied_holds, 0);
        assert_eq!(outcome.retention_seconds, None);
    }

    #[test]
    fn duplicate_webhook_outcome_for_renamed() {
        let new_repo = RepositoryRef::new(ProviderKind::GitHub, "new-org", "new-repo").unwrap();
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap(),
            WebhookDeliveryId::new("delivery-dup-rename").unwrap(),
            RepositoryWebhookEventKind::RepositoryRenamed {
                new_repository: new_repo,
            },
        );
        let outcome = super::duplicate_webhook_outcome(&event);
        assert_eq!(
            outcome.event_kind,
            ProviderWebhookOutcomeKind::RepositoryRenamed {
                new_owner: "new-org".to_owned(),
                new_repo: "new-repo".to_owned(),
            }
        );
    }

    #[test]
    fn duplicate_webhook_outcome_for_revision_pushed() {
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap(),
            WebhookDeliveryId::new("delivery-dup-rev").unwrap(),
            RepositoryWebhookEventKind::RevisionPushed {
                revision: RevisionRef::new("refs/heads/main").unwrap(),
            },
        );
        let outcome = super::duplicate_webhook_outcome(&event);
        assert_eq!(
            outcome.event_kind,
            ProviderWebhookOutcomeKind::RevisionPushed {
                revision: "refs/heads/main".to_owned(),
            }
        );
    }

    #[test]
    fn duplicate_webhook_outcome_for_access_changed() {
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap(),
            WebhookDeliveryId::new("delivery-dup-access").unwrap(),
            RepositoryWebhookEventKind::AccessChanged,
        );
        let outcome = super::duplicate_webhook_outcome(&event);
        assert_eq!(
            outcome.event_kind,
            ProviderWebhookOutcomeKind::AccessChanged
        );
    }
}

#[cfg(test)]
mod error_display_tests {
    use super::ProviderEventsError;
    use shardline_index::RetentionHoldError;

    #[test]
    fn provider_events_error_display_all_variants() {
        let cases: &[(ProviderEventsError, &str)] = &[
            (ProviderEventsError::Overflow, "overflow"),
            (ProviderEventsError::InvalidContentHash, "hash"),
            (ProviderEventsError::InvalidProviderWebhookPayload, "payload"),
            (ProviderEventsError::ConflictingRenameTargetRecord, "conflicting"),
            (ProviderEventsError::Json(serde_json::from_str::<serde_json::Value>("invalid json...").unwrap_err()), "json"),
            (ProviderEventsError::NumericConversion(u64::try_from(-1i32).unwrap_err()), "bounds"),
            (ProviderEventsError::RetentionHold(RetentionHoldError::EmptyReason), "hold"),
            (ProviderEventsError::WebhookDelivery(shardline_index::WebhookDeliveryError::EmptyDeliveryId), "delivery"),
            (ProviderEventsError::ObjectStore(shardline_server_core::ServerObjectStoreError::NotFound), "object"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn provider_events_error_xet_adapter_display() {
        let error = ProviderEventsError::XetAdapter(
            shardline_xet_adapter::XetAdapterError::NotFound
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn provider_events_error_index_store_display() {
        let error = ProviderEventsError::IndexStore(
            shardline_index::LocalIndexStoreError::InvalidLegacyImportState
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn provider_events_error_memory_index_store_display() {
        let error = ProviderEventsError::MemoryIndexStore(
            shardline_index::MemoryIndexStoreError::LockPoisoned
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn provider_events_error_memory_record_store_display() {
        let error = ProviderEventsError::MemoryRecordStore(
            shardline_index::MemoryRecordStoreError::LockPoisoned
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn provider_events_error_parse_stored_file_record_display() {
        let error = ProviderEventsError::ParseStoredFileRecord(
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes: 999,
                maximum_bytes: 100,
            }
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn provider_events_error_postgres_metadata_display() {
        let error = ProviderEventsError::PostgresMetadata(
            shardline_index::PostgresMetadataStoreError::HashParse(
                shardline_protocol::HashParseError::InvalidCharacter,
            ),
        );
        let msg = error.to_string();
        assert_eq!(msg, "postgres metadata adapter operation failed");
    }

    #[test]
    fn provider_events_error_xet_adapter_display_nonempty() {
        let error = ProviderEventsError::XetAdapter(
            shardline_xet_adapter::XetAdapterError::NotFound,
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("xet"));
    }

    #[test]
    fn provider_events_error_index_store_display_nonempty() {
        let error = ProviderEventsError::IndexStore(
            shardline_index::LocalIndexStoreError::InvalidLegacyImportState,
        );
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }
}

fn duplicate_webhook_outcome(event: &RepositoryWebhookEvent) -> ProviderWebhookOutcome {
    ProviderWebhookOutcome {
        provider: event.repository().provider(),
        owner: event.repository().owner().to_owned(),
        repo: event.repository().name().to_owned(),
        delivery_id: event.delivery_id().as_str().to_owned(),
        event_kind: outcome::duplicate_webhook_event_kind(event.kind()),
        affected_file_versions: 0,
        affected_chunks: 0,
        applied_holds: 0,
        retention_seconds: None,
    }
}

/// Applies a normalized provider webhook to Shardline lifecycle state.
///
/// # Errors
///
/// Returns [`ProviderEventsError`] when record or index storage cannot be read
/// or updated.
pub async fn apply_provider_webhook_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &shardline_server_core::ServerObjectStore,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let recorded_delivery = WebhookDelivery::new(
        event.repository().provider().repository_provider(),
        event.repository().owner().to_owned(),
        event.repository().name().to_owned(),
        event.delivery_id().as_str().to_owned(),
        unix_now_seconds_lossy(),
    )?;
    if !index_store
        .record_webhook_delivery(&recorded_delivery)
        .await
        .map_err(Into::into)?
    {
        return Ok(duplicate_webhook_outcome(event));
    }

    let outcome = match event.kind() {
        RepositoryWebhookEventKind::RepositoryDeleted => {
            repository::apply_repository_deleted(record_store, index_store, object_store, event)
                .await
        }
        RepositoryWebhookEventKind::RepositoryRenamed { new_repository } => {
            repository::apply_repository_renamed(record_store, index_store, event, new_repository)
                .await
        }
        RepositoryWebhookEventKind::AccessChanged => {
            state::apply_access_changed(
                index_store,
                event,
                recorded_delivery.processed_at_unix_seconds(),
            )
            .await
        }
        RepositoryWebhookEventKind::RevisionPushed { revision } => {
            state::apply_revision_pushed(
                index_store,
                event,
                revision.as_str(),
                recorded_delivery.processed_at_unix_seconds(),
            )
            .await
        }
    };

    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            if let Err(delete_err) = index_store
                .delete_webhook_delivery(&recorded_delivery)
                .await
                .map_err(Into::into)
            {
                tracing::warn!(
                    delivery_id = recorded_delivery.delivery_id(),
                    owner = recorded_delivery.owner(),
                    repo = recorded_delivery.repo(),
                    "failed to remove webhook delivery record for retry: {delete_err}"
                );
            }
            Err(error)
        }
    }
}
