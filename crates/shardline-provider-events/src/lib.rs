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
//!
//! # Quick start
//!
//! The outcome types are pure data describing what one webhook application
//! did, so they are the easiest entry point:
//!
//! ```
//! use shardline_provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};
//! use shardline_vcs::ProviderKind;
//!
//! let outcome = ProviderWebhookOutcome {
//!     provider: ProviderKind::GitHub,
//!     owner: "acme".to_owned(),
//!     repo: "assets".to_owned(),
//!     delivery_id: "delivery-123".to_owned(),
//!     event_kind: ProviderWebhookOutcomeKind::RepositoryDeleted,
//!     affected_file_versions: 12,
//!     affected_chunks: 34,
//!     applied_holds: 1,
//!     retention_seconds: Some(3600),
//! };
//!
//! assert_eq!(outcome.owner, "acme");
//! assert!(matches!(outcome.event_kind, ProviderWebhookOutcomeKind::RepositoryDeleted));
//! ```
//!
//! A rename carries the new location:
//!
//! ```
//! use shardline_provider_events::ProviderWebhookOutcomeKind;
//!
//! let kind = ProviderWebhookOutcomeKind::RepositoryRenamed {
//!     new_owner: "acme".to_owned(),
//!     new_repo: "assets-v2".to_owned(),
//! };
//! assert!(matches!(
//!     kind,
//!     ProviderWebhookOutcomeKind::RepositoryRenamed { new_repo, .. } if new_repo == "assets-v2"
//! ));
//! ```
//!
//! To apply a webhook to real state, call [`apply_provider_webhook_with_stores`]
//! with explicit record, index, and object-store adapters.

use std::num::TryFromIntError;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Default retention window for the provider webhook delivery replay-dedup
/// table. Delivery claims older than this are purged to bound metadata-store
/// growth; replay dedup within the window is unaffected.
pub const WEBHOOK_DELIVERY_RETENTION_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Minimum interval between opportunistic dedup-table purges.
const WEBHOOK_DELIVERY_PURGE_INTERVAL_SECONDS: u64 = 60 * 60;

/// Last time (unix seconds) the delivery-dedup table was purged, so the purge
/// runs at most once per [`WEBHOOK_DELIVERY_PURGE_INTERVAL_SECONDS`] per
/// process instead of on every webhook event.
static LAST_WEBHOOK_DELIVERY_PURGE_AT_UNIX_SECONDS: AtomicU64 = AtomicU64::new(0);

mod outcome;
mod records;
mod repository;
mod state;
#[cfg(test)]
mod tests;

/// Summary of one handled provider webhook.
///
/// Returned by [`apply_provider_webhook_with_stores`] for each non-duplicate
/// webhook. Reports which repository was affected, what kind of mutation was
/// applied, and how much durable state changed.
///
/// # Examples
///
/// ```
/// use shardline_provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};
/// use shardline_vcs::ProviderKind;
///
/// let outcome = ProviderWebhookOutcome {
///     provider: ProviderKind::GitLab,
///     owner: "acme".to_owned(),
///     repo: "assets".to_owned(),
///     delivery_id: "delivery-42".to_owned(),
///     event_kind: ProviderWebhookOutcomeKind::AccessChanged,
///     affected_file_versions: 0,
///     affected_chunks: 0,
///     applied_holds: 0,
///     retention_seconds: None,
/// };
/// assert_eq!(outcome.delivery_id, "delivery-42");
/// ```
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

/// Opportunistically purges webhook-delivery dedup rows older than the
/// retention window, at most once per
/// [`WEBHOOK_DELIVERY_PURGE_INTERVAL_SECONDS`].
///
/// Runs on the delivery-record path so the table stays bounded without a
/// dedicated scheduler task. Best-effort: failures are logged and ignored so
/// a purge hiccup never fails a webhook that has already been applied.
async fn purge_expired_webhook_deliveries<IndexAdapter>(index_store: &IndexAdapter)
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let now = unix_now_seconds_lossy();
    let last_purge = LAST_WEBHOOK_DELIVERY_PURGE_AT_UNIX_SECONDS.load(Ordering::Relaxed);
    if now.saturating_sub(last_purge) < WEBHOOK_DELIVERY_PURGE_INTERVAL_SECONDS {
        return;
    }
    // Claim the purge slot before running it so concurrent webhook events do
    // not all issue a range DELETE in the same interval.
    if LAST_WEBHOOK_DELIVERY_PURGE_AT_UNIX_SECONDS
        .compare_exchange(last_purge, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    let older_than = now.saturating_sub(WEBHOOK_DELIVERY_RETENTION_SECONDS);
    match index_store
        .purge_webhook_deliveries_older_than(older_than)
        .await
    {
        Ok(purged) if purged > 0 => {
            tracing::info!(
                purged,
                older_than_unix_seconds = older_than,
                "purged expired provider webhook delivery dedup rows"
            );
        }
        Ok(_) => {}
        Err(error) => {
            let error: ProviderEventsError = error.into();
            tracing::warn!("failed to purge expired provider webhook delivery dedup rows: {error}");
        }
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
    purge_expired_webhook_deliveries(index_store).await;

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
