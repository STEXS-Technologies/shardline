//! Bounded, verified inventories read from one PostgreSQL snapshot.

use std::future::Future;

use shardline_reliability::{
    QuarantineLifecycleState, RetentionHoldLifecycleState, SnapshotEvidence,
    WebhookDeliveryLifecycleState,
};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow, query};

use super::{
    PostgresMetadataStoreError, ProviderRepositoryState, QuarantineCandidate, RetentionHold,
    WebhookDelivery, provider_repository_state_from_row, quarantine_candidate_from_row,
    quarantine_snapshot, retention_hold_from_row, retention_snapshot,
    verify_postgres_provider_evidence_batch, verify_postgres_quarantine_evidence_batch,
    verify_postgres_retention_evidence_batch, verify_postgres_webhook_evidence_batch,
    webhook_delivery_from_row, webhook_snapshot,
};

pub(super) trait InventoryEntry<const KEY_FIELDS: usize>: Sized + Send + Sync {
    const FIRST_PAGE: &'static str;
    const NEXT_PAGE: &'static str;
    const KEY_COLUMNS: [&'static str; KEY_FIELDS];
    const DECODE: fn(&PgRow) -> Result<Self, PostgresMetadataStoreError>;

    fn validate_identity(&self) -> Result<(), PostgresMetadataStoreError>;
    fn verify_batch(
        connection: &mut PgConnection,
        entries: &[Self],
        verify_materialization: bool,
    ) -> impl Future<Output = Result<(), PostgresMetadataStoreError>> + Send;
}

#[derive(Clone, Copy)]
enum Phase {
    Rows,
    Identities,
    EvidenceHeads,
    Materialization,
    Callbacks,
}

pub(super) async fn visit<Entry, Visitor, VisitorError, const KEY_FIELDS: usize>(
    pool: &PgPool,
    mut visitor: Visitor,
) -> Result<(), VisitorError>
where
    Entry: InventoryEntry<KEY_FIELDS>,
    PostgresMetadataStoreError: Into<VisitorError>,
    Visitor: FnMut(Entry) -> Result<(), VisitorError> + Send,
    VisitorError: Send,
{
    let mut transaction = pool
        .begin()
        .await
        .map_err(PostgresMetadataStoreError::from)
        .map_err(Into::into)?;
    query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(PostgresMetadataStoreError::from)
        .map_err(Into::into)?;
    // Complete each validation pass across every page before beginning the next.
    // Callbacks see no entries until all materialized rows and evidence agree.
    for phase in [
        Phase::Rows,
        Phase::Identities,
        Phase::EvidenceHeads,
        Phase::Materialization,
        Phase::Callbacks,
    ] {
        let mut cursor: Option<[String; KEY_FIELDS]> = None;
        loop {
            let mut request = query(if cursor.is_some() {
                Entry::NEXT_PAGE
            } else {
                Entry::FIRST_PAGE
            });
            if let Some(key) = &cursor {
                for field in key {
                    request = request.bind(field);
                }
            }
            let rows = request
                .fetch_all(&mut *transaction)
                .await
                .map_err(PostgresMetadataStoreError::from)
                .map_err(Into::into)?;
            if rows.is_empty() {
                break;
            }
            let mut entries = Vec::with_capacity(rows.len());
            for row in rows {
                let entry = Entry::DECODE(&row).map_err(Into::into)?;
                // Stored keys keep pagination consistent with SQL ordering.
                let mut key = std::array::from_fn(|_| String::new());
                for (value, column) in key.iter_mut().zip(Entry::KEY_COLUMNS) {
                    *value = row
                        .try_get::<String, _>(column)
                        .map_err(PostgresMetadataStoreError::from)
                        .map_err(Into::into)?;
                }
                cursor = Some(key);
                entries.push(entry);
            }
            match phase {
                Phase::Rows => {}
                Phase::Identities => {
                    for entry in &entries {
                        entry.validate_identity().map_err(Into::into)?;
                    }
                }
                Phase::EvidenceHeads | Phase::Materialization => {
                    Entry::verify_batch(
                        &mut transaction,
                        &entries,
                        matches!(phase, Phase::Materialization),
                    )
                    .await
                    .map_err(Into::into)?;
                }
                Phase::Callbacks => {
                    for entry in entries {
                        visitor(entry)?;
                    }
                }
            }
        }
    }
    transaction
        .commit()
        .await
        .map_err(PostgresMetadataStoreError::from)
        .map_err(Into::into)
}

impl InventoryEntry<1> for QuarantineCandidate {
    const FIRST_PAGE: &'static str = "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds FROM shardline_quarantine_candidates ORDER BY object_key LIMIT 256";
    const NEXT_PAGE: &'static str = "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds FROM shardline_quarantine_candidates
                               WHERE object_key > $1 ORDER BY object_key LIMIT 256";
    const KEY_COLUMNS: [&'static str; 1] = ["object_key"];
    const DECODE: fn(&PgRow) -> Result<Self, PostgresMetadataStoreError> =
        quarantine_candidate_from_row;

    fn validate_identity(&self) -> Result<(), PostgresMetadataStoreError> {
        quarantine_snapshot(self, QuarantineLifecycleState::Active)?.evidence_operation()?;
        Ok(())
    }

    async fn verify_batch(
        connection: &mut PgConnection,
        entries: &[Self],
        verify_materialization: bool,
    ) -> Result<(), PostgresMetadataStoreError> {
        verify_postgres_quarantine_evidence_batch(connection, entries, verify_materialization).await
    }
}

impl InventoryEntry<1> for RetentionHold {
    const FIRST_PAGE: &'static str = "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds FROM shardline_retention_holds ORDER BY object_key LIMIT 256";
    const NEXT_PAGE: &'static str = "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds FROM shardline_retention_holds
                               WHERE object_key > $1 ORDER BY object_key LIMIT 256";
    const KEY_COLUMNS: [&'static str; 1] = ["object_key"];
    const DECODE: fn(&PgRow) -> Result<Self, PostgresMetadataStoreError> = retention_hold_from_row;

    fn validate_identity(&self) -> Result<(), PostgresMetadataStoreError> {
        retention_snapshot(self, RetentionHoldLifecycleState::Active)?.evidence_operation()?;
        Ok(())
    }

    async fn verify_batch(
        connection: &mut PgConnection,
        entries: &[Self],
        verify_materialization: bool,
    ) -> Result<(), PostgresMetadataStoreError> {
        verify_postgres_retention_evidence_batch(connection, entries, verify_materialization).await
    }
}

impl InventoryEntry<4> for WebhookDelivery {
    const FIRST_PAGE: &'static str = "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds FROM shardline_webhook_deliveries
                        ORDER BY provider, owner, repo, delivery_id LIMIT 256";
    const NEXT_PAGE: &'static str = "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds FROM shardline_webhook_deliveries
                        WHERE (provider, owner, repo, delivery_id) > ($1, $2, $3, $4)
                        ORDER BY provider, owner, repo, delivery_id LIMIT 256";
    const KEY_COLUMNS: [&'static str; 4] = ["provider", "owner", "repo", "delivery_id"];
    const DECODE: fn(&PgRow) -> Result<Self, PostgresMetadataStoreError> =
        webhook_delivery_from_row;

    fn validate_identity(&self) -> Result<(), PostgresMetadataStoreError> {
        webhook_snapshot(self, WebhookDeliveryLifecycleState::Processed)?.evidence_operation()?;
        Ok(())
    }

    async fn verify_batch(
        connection: &mut PgConnection,
        entries: &[Self],
        verify_materialization: bool,
    ) -> Result<(), PostgresMetadataStoreError> {
        verify_postgres_webhook_evidence_batch(connection, entries, verify_materialization).await
    }
}

impl InventoryEntry<3> for ProviderRepositoryState {
    const FIRST_PAGE: &'static str = "SELECT provider, owner, repo, last_access_changed_at_unix_seconds, last_revision_pushed_at_unix_seconds, last_pushed_revision, last_cache_invalidated_at_unix_seconds, last_authorization_rechecked_at_unix_seconds, last_drift_checked_at_unix_seconds FROM shardline_provider_repository_states
                        ORDER BY provider, owner, repo LIMIT 256";
    const NEXT_PAGE: &'static str = "SELECT provider, owner, repo, last_access_changed_at_unix_seconds, last_revision_pushed_at_unix_seconds, last_pushed_revision, last_cache_invalidated_at_unix_seconds, last_authorization_rechecked_at_unix_seconds, last_drift_checked_at_unix_seconds FROM shardline_provider_repository_states
                        WHERE (provider, owner, repo) > ($1, $2, $3)
                        ORDER BY provider, owner, repo LIMIT 256";
    const KEY_COLUMNS: [&'static str; 3] = ["provider", "owner", "repo"];
    const DECODE: fn(&PgRow) -> Result<Self, PostgresMetadataStoreError> =
        provider_repository_state_from_row;

    fn validate_identity(&self) -> Result<(), PostgresMetadataStoreError> {
        crate::provider_evidence::snapshot_from_state(self)?.evidence_operation()?;
        Ok(())
    }

    async fn verify_batch(
        connection: &mut PgConnection,
        entries: &[Self],
        verify_materialization: bool,
    ) -> Result<(), PostgresMetadataStoreError> {
        verify_postgres_provider_evidence_batch(connection, entries, verify_materialization).await
    }
}
