use shardline_protocol::RepositoryProvider;
use shardline_reliability::{
    ProviderEvidenceLog, ProviderLifecycleEvent, ProviderLifecycleSnapshot,
    ProviderRepositoryOperationId, RetentionEvidenceLog, RetentionHoldLifecycleState,
    SnapshotEvidence, WebhookDeliveryEvidenceLog, WebhookDeliveryLifecycleState,
    append_or_baseline_snapshot_evidence, verify_and_append_snapshot_transition,
    verify_or_repair_snapshot_evidence, verify_provider_lifecycle_events,
};
use sqlx::{Acquire, PgConnection, Postgres, Row, Transaction, query, query_scalar};

use super::{
    PostgresMetadataStoreError, PostgresRecordLocator, RecordKind,
    record_store::{record_locator, upsert_record_in_transaction},
    u64_to_i64,
};
use crate::{
    FileRecord, ProviderRepositoryState, ResourceLockKey, RetentionHold, WebhookDelivery,
    provider_evidence::snapshot_from_state,
};

/// One durable fencing identity that must still match when a provider mutation commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresResourceFence {
    key: ResourceLockKey,
    epoch: i64,
}

impl PostgresResourceFence {
    /// Creates an expected resource-fence identity.
    #[must_use]
    pub const fn new(key: ResourceLockKey, epoch: i64) -> Self {
        Self { key, epoch }
    }
}

/// Primary-key identity for provider repository lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRepositoryKey {
    provider: RepositoryProvider,
    owner: String,
    repo: String,
}

impl ProviderRepositoryKey {
    /// Creates a provider repository key.
    #[must_use]
    pub const fn new(provider: RepositoryProvider, owner: String, repo: String) -> Self {
        Self {
            provider,
            owner,
            repo,
        }
    }
}

/// All durable metadata changes produced by one repository rename or deletion webhook.
#[derive(Debug, Clone)]
pub struct PostgresProviderMutation {
    delivery: WebhookDelivery,
    record_upserts: Vec<FileRecord>,
    record_deletes: Vec<String>,
    retention_holds: Vec<RetentionHold>,
    state_upserts: Vec<ProviderRepositoryState>,
    state_deletes: Vec<ProviderRepositoryKey>,
}

impl PostgresProviderMutation {
    /// Creates an empty mutation anchored to the webhook delivery claim.
    #[must_use]
    pub const fn new(delivery: WebhookDelivery) -> Self {
        Self {
            delivery,
            record_upserts: Vec::new(),
            record_deletes: Vec::new(),
            retention_holds: Vec::new(),
            state_upserts: Vec::new(),
            state_deletes: Vec::new(),
        }
    }

    /// Adds a file record whose version and latest aliases must be written atomically.
    pub fn upsert_record(&mut self, record: FileRecord) {
        self.record_upserts.push(record);
    }

    /// Adds an existing file-record locator to remove.
    pub fn delete_record(&mut self, locator: &PostgresRecordLocator) {
        self.record_deletes.push(locator.record_key().to_owned());
    }

    /// Adds or refreshes a retention hold.
    pub fn upsert_retention_hold(&mut self, hold: RetentionHold) {
        self.retention_holds.push(hold);
    }

    /// Returns the number of retention holds in this mutation.
    #[must_use]
    pub const fn retention_holds_len(&self) -> usize {
        self.retention_holds.len()
    }

    /// Adds or monotonically merges provider repository lifecycle state.
    pub fn upsert_provider_repository_state(&mut self, state: ProviderRepositoryState) {
        self.state_upserts.push(state);
    }

    /// Removes provider repository lifecycle state at commit.
    pub fn delete_provider_repository_state(&mut self, key: ProviderRepositoryKey) {
        self.state_deletes.push(key);
    }
}

/// Result of attempting one fenced provider metadata transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresProviderMutationOutcome {
    /// The delivery claim and every mutation committed together.
    Applied,
    /// The delivery was already committed, so no mutation was re-applied.
    Duplicate,
    /// At least one expected ownership epoch had already been superseded.
    StaleFence,
}

impl super::PostgresIndexStore {
    /// Atomically commits one provider rename/delete mutation on the lock-owning connection.
    ///
    /// Fence rows are locked before any metadata mutation. A replacement writer cannot
    /// advance an epoch until this transaction commits or aborts, including when a rename
    /// owns a second repository through another advisory-lock session.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be completed.
    pub async fn commit_provider_mutation_on_connection(
        connection: &mut PgConnection,
        expected_fences: &[PostgresResourceFence],
        mutation: &PostgresProviderMutation,
    ) -> Result<PostgresProviderMutationOutcome, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        for fence in expected_fences {
            let epoch = query_scalar::<_, i64>(
                "SELECT epoch
                 FROM shardline_resource_fences
                 WHERE domain = $1 AND resource = $2
                 FOR UPDATE",
            )
            .bind(fence.key.domain().as_str())
            .bind(fence.key.resource())
            .fetch_optional(&mut *transaction)
            .await?;
            if epoch != Some(fence.epoch) {
                transaction.rollback().await?;
                return Ok(PostgresProviderMutationOutcome::StaleFence);
            }
        }

        let inserted = record_webhook_delivery(&mut transaction, &mutation.delivery).await?;
        if !inserted {
            transaction.rollback().await?;
            return Ok(PostgresProviderMutationOutcome::Duplicate);
        }

        for hold in &mutation.retention_holds {
            upsert_retention_hold(&mut transaction, hold).await?;
        }
        for record in &mutation.record_upserts {
            let version = record_locator(
                RecordKind::Version,
                record,
                Some(record.content_hash.clone()),
            );
            upsert_record_in_transaction(&mut transaction, &version, record).await?;
            let latest = record_locator(RecordKind::Latest, record, None);
            upsert_record_in_transaction(&mut transaction, &latest, record).await?;
        }
        for record_key in &mutation.record_deletes {
            query("DELETE FROM shardline_file_records WHERE record_key = $1")
                .bind(record_key)
                .execute(&mut *transaction)
                .await?;
        }
        for state in &mutation.state_upserts {
            upsert_provider_repository_state(&mut transaction, state).await?;
        }
        for key in &mutation.state_deletes {
            let operation_id =
                ProviderRepositoryOperationId::new(key.provider.as_str(), &key.owner, &key.repo);
            query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(operation_id.as_str())
                .execute(&mut *transaction)
                .await?;
            let current = query(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = $1 AND owner = $2 AND repo = $3
                 FOR UPDATE",
            )
            .bind(key.provider.as_str())
            .bind(&key.owner)
            .bind(&key.repo)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = current {
                let state = super::index_store::provider_repository_state_from_row(&row)?;
                verify_provider_repository_state_evidence(&mut transaction, &state).await?;
            }
            query(
                "DELETE FROM shardline_provider_repository_states
                 WHERE provider = $1 AND owner = $2 AND repo = $3",
            )
            .bind(key.provider.as_str())
            .bind(&key.owner)
            .bind(&key.repo)
            .execute(&mut *transaction)
            .await?;
            query(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
            )
            .bind(operation_id.as_str())
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;
        Ok(PostgresProviderMutationOutcome::Applied)
    }
}

pub(super) async fn record_webhook_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &WebhookDelivery,
) -> Result<bool, PostgresMetadataStoreError> {
    let existing_row = query(
        "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
         FROM shardline_webhook_deliveries
         WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4
         FOR UPDATE",
    )
    .bind(delivery.provider().as_str())
    .bind(delivery.owner())
    .bind(delivery.repo())
    .bind(delivery.delivery_id())
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = existing_row {
        let existing = super::index_store::webhook_delivery_from_row(&row)?;
        let snapshot = super::index_store::webhook_snapshot(
            &existing,
            WebhookDeliveryLifecycleState::Processed,
        )?;
        let evidence =
            super::index_store::load_postgres_webhook_evidence(&mut **transaction, &existing)
                .await?;
        let (_evidence, _) = verify_or_repair_snapshot_evidence(evidence, snapshot)?;
        return Ok(false);
    }
    let snapshot =
        super::index_store::webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
    let evidence =
        super::index_store::load_postgres_webhook_evidence(&mut **transaction, delivery).await?;
    let (evidence, _evidence_was_empty) = if evidence.events().is_empty() {
        (WebhookDeliveryEvidenceLog::baseline(snapshot)?, true)
    } else {
        let released = super::index_store::webhook_snapshot(
            delivery,
            WebhookDeliveryLifecycleState::Released,
        )?;
        verify_and_append_snapshot_transition(evidence, released, snapshot)?
    };
    query(
        "INSERT INTO shardline_webhook_deliveries (
            provider, owner, repo, delivery_id, processed_at_unix_seconds
         ) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(delivery.provider().as_str())
    .bind(delivery.owner())
    .bind(delivery.repo())
    .bind(delivery.delivery_id())
    .bind(u64_to_i64(delivery.processed_at_unix_seconds())?)
    .execute(&mut **transaction)
    .await?;
    for event in evidence.events() {
        super::insert_reliability_event(&mut **transaction, event).await?;
    }
    Ok(true)
}

pub(super) async fn upsert_retention_hold(
    transaction: &mut Transaction<'_, Postgres>,
    hold: &RetentionHold,
) -> Result<(), PostgresMetadataStoreError> {
    let previous_row = query(
        "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
         FROM shardline_retention_holds WHERE object_key = $1 FOR UPDATE",
    )
    .bind(hold.object_key().as_str())
    .fetch_optional(&mut **transaction)
    .await?;
    let previous = previous_row
        .as_ref()
        .map(super::index_store::retention_hold_from_row)
        .transpose()?;
    let snapshot =
        super::index_store::retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
    let evidence = super::index_store::load_postgres_retention_evidence(
        &mut **transaction,
        hold.object_key().as_str(),
    )
    .await?;
    let (evidence, evidence_was_empty) = if evidence.events().is_empty() {
        (RetentionEvidenceLog::baseline(snapshot.clone())?, true)
    } else if let Some(previous) = previous.as_ref() {
        let previous_snapshot =
            super::index_store::retention_snapshot(previous, RetentionHoldLifecycleState::Active)?;
        verify_and_append_snapshot_transition(evidence, previous_snapshot, snapshot.clone())?
    } else {
        let released =
            super::index_store::retention_snapshot(hold, RetentionHoldLifecycleState::Released)?;
        verify_and_append_snapshot_transition(evidence, released, snapshot.clone())?
    };
    query(
        "INSERT INTO shardline_retention_holds (
            object_key,
            reason,
            held_at_unix_seconds,
            release_after_unix_seconds
         )
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (object_key)
         DO UPDATE SET
            reason = EXCLUDED.reason,
            held_at_unix_seconds = EXCLUDED.held_at_unix_seconds,
            release_after_unix_seconds = EXCLUDED.release_after_unix_seconds",
    )
    .bind(hold.object_key().as_str())
    .bind(hold.reason())
    .bind(u64_to_i64(hold.held_at_unix_seconds())?)
    .bind(
        hold.release_after_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .execute(&mut **transaction)
    .await?;
    if evidence_was_empty {
        for event in evidence.events() {
            super::insert_reliability_event(&mut **transaction, event).await?;
        }
    } else if let Some(event) = evidence.events().last() {
        super::insert_reliability_event(&mut **transaction, event).await?;
    }
    Ok(())
}

pub(super) async fn upsert_provider_repository_state(
    transaction: &mut Transaction<'_, Postgres>,
    state: &ProviderRepositoryState,
) -> Result<(), PostgresMetadataStoreError> {
    let operation_id = format!(
        "{}:{}:{}",
        state.provider().as_str(),
        state.owner(),
        state.repo()
    );
    query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&operation_id)
        .execute(&mut **transaction)
        .await?;
    let current = query(
        "SELECT provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds
         FROM shardline_provider_repository_states
         WHERE provider = $1 AND owner = $2 AND repo = $3
         FOR UPDATE",
    )
    .bind(state.provider().as_str())
    .bind(state.owner())
    .bind(state.repo())
    .fetch_optional(&mut **transaction)
    .await?;
    let current_snapshot = current
        .as_ref()
        .map(super::index_store::provider_repository_state_from_row)
        .transpose()?
        .map(|current_state| snapshot_from_state(&current_state))
        .transpose()?;
    let evidence = if let Some(snapshot) = current_snapshot.as_ref() {
        ProviderEvidenceLog::from_events(load_provider_evidence(transaction, snapshot).await?)?
    } else {
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(&operation_id)
        .execute(&mut **transaction)
        .await?;
        ProviderEvidenceLog::default()
    };
    query(
        "INSERT INTO shardline_provider_repository_states (
            provider,
            owner,
            repo,
            last_access_changed_at_unix_seconds,
            last_revision_pushed_at_unix_seconds,
            last_pushed_revision,
            last_cache_invalidated_at_unix_seconds,
            last_authorization_rechecked_at_unix_seconds,
            last_drift_checked_at_unix_seconds
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (provider, owner, repo)
         DO UPDATE SET
            last_access_changed_at_unix_seconds = CASE
                WHEN EXCLUDED.last_access_changed_at_unix_seconds IS NULL
                    THEN shardline_provider_repository_states.last_access_changed_at_unix_seconds
                WHEN shardline_provider_repository_states.last_access_changed_at_unix_seconds IS NULL
                  OR EXCLUDED.last_access_changed_at_unix_seconds >= shardline_provider_repository_states.last_access_changed_at_unix_seconds
                    THEN EXCLUDED.last_access_changed_at_unix_seconds
                ELSE shardline_provider_repository_states.last_access_changed_at_unix_seconds
            END,
            last_pushed_revision = CASE
                WHEN EXCLUDED.last_revision_pushed_at_unix_seconds IS NOT NULL
                 AND (shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                   OR EXCLUDED.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds)
                    THEN EXCLUDED.last_pushed_revision
                ELSE shardline_provider_repository_states.last_pushed_revision
            END,
            last_revision_pushed_at_unix_seconds = CASE
                WHEN EXCLUDED.last_revision_pushed_at_unix_seconds IS NULL
                    THEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                WHEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                  OR EXCLUDED.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                    THEN EXCLUDED.last_revision_pushed_at_unix_seconds
                ELSE shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
            END,
            last_cache_invalidated_at_unix_seconds = CASE
                WHEN EXCLUDED.last_cache_invalidated_at_unix_seconds IS NULL
                    THEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                WHEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds IS NULL
                  OR EXCLUDED.last_cache_invalidated_at_unix_seconds >= shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                    THEN EXCLUDED.last_cache_invalidated_at_unix_seconds
                ELSE shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
            END,
            last_authorization_rechecked_at_unix_seconds = CASE
                WHEN EXCLUDED.last_authorization_rechecked_at_unix_seconds IS NULL
                    THEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                WHEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds IS NULL
                  OR EXCLUDED.last_authorization_rechecked_at_unix_seconds >= shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                    THEN EXCLUDED.last_authorization_rechecked_at_unix_seconds
                ELSE shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
            END,
            last_drift_checked_at_unix_seconds = CASE
                WHEN EXCLUDED.last_drift_checked_at_unix_seconds IS NULL
                    THEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                WHEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds IS NULL
                  OR EXCLUDED.last_drift_checked_at_unix_seconds >= shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                    THEN EXCLUDED.last_drift_checked_at_unix_seconds
                ELSE shardline_provider_repository_states.last_drift_checked_at_unix_seconds
            END,
            updated_at = now()",
    )
    .bind(state.provider().as_str())
    .bind(state.owner())
    .bind(state.repo())
    .bind(
        state
            .last_access_changed_at_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .bind(
        state
            .last_revision_pushed_at_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .bind(state.last_pushed_revision())
    .bind(
        state
            .last_cache_invalidated_at_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .bind(
        state
            .last_authorization_rechecked_at_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .bind(
        state
            .last_drift_checked_at_unix_seconds()
            .map(u64_to_i64)
            .transpose()?,
    )
    .execute(&mut **transaction)
    .await?;
    let row = query(
        "SELECT provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds
         FROM shardline_provider_repository_states
         WHERE provider = $1 AND owner = $2 AND repo = $3",
    )
    .bind(state.provider().as_str())
    .bind(state.owner())
    .bind(state.repo())
    .fetch_one(&mut **transaction)
    .await?;
    let merged_state = super::index_store::provider_repository_state_from_row(&row)?;
    let snapshot = snapshot_from_state(&merged_state)?;
    let evidence = if let Some(before) = current_snapshot {
        verify_and_append_snapshot_transition(evidence, before, snapshot)?.0
    } else {
        append_or_baseline_snapshot_evidence(evidence, snapshot)?
    };
    let event = evidence.events().last().ok_or_else(|| {
        PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::EmptyField("provider evidence"),
        )
    })?;
    super::insert_reliability_event(&mut **transaction, event).await?;
    Ok(())
}

async fn load_provider_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &ProviderLifecycleSnapshot,
) -> Result<Vec<ProviderLifecycleEvent>, PostgresMetadataStoreError> {
    let operation_id = snapshot.evidence_operation()?.operation_id;
    let rows = query(
        "SELECT event_json
         FROM shardline_reliability_events
         WHERE operation_kind = 'ProviderEvent' AND operation_id = $1
         ORDER BY sequence",
    )
    .bind(operation_id)
    .fetch_all(&mut **transaction)
    .await?;
    rows.into_iter()
        .map(|row| Ok(serde_json::from_value(row.try_get("event_json")?)?))
        .collect()
}

pub(super) async fn verify_provider_repository_state_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    state: &ProviderRepositoryState,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = snapshot_from_state(state)?;
    let evidence = load_provider_evidence(transaction, &snapshot).await?;
    if evidence.is_empty() {
        let baseline = ProviderEvidenceLog::baseline(snapshot.clone())?;
        verify_provider_lifecycle_events(baseline.events(), &snapshot)?;
        let event = baseline.events().last().ok_or_else(|| {
            PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::EmptyField("provider evidence"),
            )
        })?;
        super::insert_reliability_event(&mut **transaction, event).await?;
    } else {
        verify_provider_lifecycle_events(&evidence, &snapshot)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use shardline_protocol::RepositoryProvider;
    use shardline_reliability::SnapshotEvidence;
    use sqlx::{PgPool, query, query_scalar};

    use super::*;

    async fn connect_postgres() -> Option<PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        PgPool::connect(&url).await.ok()
    }

    fn delivery(owner: &str, repo: &str, id: &str) -> WebhookDelivery {
        WebhookDelivery::new(
            RepositoryProvider::GitHub,
            owner.to_owned(),
            repo.to_owned(),
            id.to_owned(),
            1_800_000_000,
        )
        .expect("valid delivery")
    }

    fn webhook_operation_id(delivery: &WebhookDelivery) -> String {
        super::super::index_store::webhook_snapshot(
            delivery,
            WebhookDeliveryLifecycleState::Processed,
        )
        .expect("valid webhook snapshot")
        .evidence_operation()
        .expect("valid webhook operation")
        .operation_id
    }

    async fn set_fence(pool: &PgPool, resource: &str, epoch: i64) {
        query(
            "INSERT INTO shardline_resource_fences (domain, resource, epoch)
             VALUES ('provider-repository', $1, $2)
             ON CONFLICT (domain, resource) DO UPDATE SET epoch = EXCLUDED.epoch",
        )
        .bind(resource)
        .bind(epoch)
        .execute(pool)
        .await
        .expect("seed fence");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_mutation_claim_and_state_commit_atomically() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner = "provider-mutation-atomic";
        let repo = "repository";
        let delivery_id = "delivery-atomic";
        let resource = "github:provider-mutation-atomic/repository";
        query(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery'
               AND (operation_id = $1 OR operation_id = $2)",
        )
        .bind(webhook_operation_id(&delivery(owner, repo, delivery_id)))
        .bind(delivery_id)
        .execute(&pool)
        .await
        .expect("clean delivery evidence fixture");
        query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean state fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(format!("github:{owner}:{repo}"))
        .execute(&pool)
        .await
        .expect("clean provider evidence fixture");
        set_fence(&pool, resource, 41).await;

        let mut mutation = PostgresProviderMutation::new(delivery(owner, repo, delivery_id));
        mutation.upsert_provider_repository_state(ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            owner.to_owned(),
            repo.to_owned(),
            Some(100),
            None,
            None,
        ));
        let fences = [PostgresResourceFence::new(
            ResourceLockKey::provider_repository("github", owner, repo),
            41,
        )];
        let mut connection = pool.acquire().await.expect("connection");
        let outcome = super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
            &mut connection,
            &fences,
            &mutation,
        )
        .await
        .expect("commit mutation");
        assert_eq!(outcome, PostgresProviderMutationOutcome::Applied);

        let delivery_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2 AND delivery_id = $3",
        )
        .bind(owner)
        .bind(repo)
        .bind(delivery_id)
        .fetch_one(&pool)
        .await
        .expect("delivery count");
        let access_changed = query_scalar::<_, Option<i64>>(
            "SELECT last_access_changed_at_unix_seconds
             FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .fetch_one(&pool)
        .await
        .expect("state timestamp");
        assert_eq!(delivery_count, 1);
        assert_eq!(access_changed, Some(100));

        let duplicate = super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
            &mut connection,
            &fences,
            &mutation,
        )
        .await
        .expect("duplicate mutation");
        assert_eq!(duplicate, PostgresProviderMutationOutcome::Duplicate);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_webhook_delivery_reclaim_replays_evidence_and_rejects_tampering() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner = "provider-webhook-evidence";
        let repo = "repository";
        let delivery_id = "delivery-recovery";
        query(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean delivery fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery'
               AND (operation_id = $1 OR operation_id = $2)",
        )
        .bind(webhook_operation_id(&delivery(owner, repo, delivery_id)))
        .bind(delivery_id)
        .execute(&pool)
        .await
        .expect("clean evidence fixture");

        let store = super::super::PostgresIndexStore::new(pool.clone());
        let delivery = delivery(owner, repo, delivery_id);
        assert!(
            crate::AsyncIndexStore::record_webhook_delivery(&store, &delivery)
                .await
                .expect("record delivery")
        );
        assert!(
            crate::AsyncIndexStore::delete_webhook_delivery(&store, &delivery)
                .await
                .expect("release delivery")
        );
        assert!(
            crate::AsyncIndexStore::record_webhook_delivery(&store, &delivery)
                .await
                .expect("reclaim delivery")
        );

        let deliveries = crate::AsyncIndexStore::list_webhook_deliveries(&store)
            .await
            .expect("list recovered delivery");
        assert_eq!(
            deliveries
                .iter()
                .filter(|candidate| *candidate == &delivery)
                .count(),
            1,
            "the recovered fixture must be present exactly once"
        );
        let event_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1",
        )
        .bind(webhook_operation_id(&delivery))
        .fetch_one(&pool)
        .await
        .expect("evidence count");
        assert_eq!(event_count, 3, "processed -> released -> processed chain");

        query(
            "UPDATE shardline_reliability_events
             SET event_json = '{\"sequence\":99}'
             WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1",
        )
        .bind(webhook_operation_id(&delivery))
        .execute(&pool)
        .await
        .expect("tamper evidence");
        assert!(
            crate::AsyncIndexStore::list_webhook_deliveries(&store)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_mutation_rolls_back_delivery_when_metadata_fails() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner = "provider-mutation-rollback";
        let repo = "repository";
        let delivery_id = "delivery-rollback";
        let resource = "github:provider-mutation-rollback/repository";
        query(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean fixture");
        set_fence(&pool, resource, 51).await;

        let mut mutation = PostgresProviderMutation::new(delivery(owner, repo, delivery_id));
        mutation.upsert_provider_repository_state(ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            String::new(),
            repo.to_owned(),
            Some(100),
            None,
            None,
        ));
        let fences = [PostgresResourceFence::new(
            ResourceLockKey::provider_repository("github", owner, repo),
            51,
        )];
        let mut connection = pool.acquire().await.expect("connection");
        let result = super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
            &mut connection,
            &fences,
            &mutation,
        )
        .await;
        assert!(result.is_err());

        let delivery_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2 AND delivery_id = $3",
        )
        .bind(owner)
        .bind(repo)
        .bind(delivery_id)
        .fetch_one(&pool)
        .await
        .expect("delivery count");
        assert_eq!(delivery_count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_mutation_rejects_stale_fence_before_delivery_claim() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner = "provider-mutation-stale";
        let repo = "repository";
        let delivery_id = "delivery-stale";
        let resource = "github:provider-mutation-stale/repository";
        query(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean fixture");
        set_fence(&pool, resource, 62).await;

        let mutation = PostgresProviderMutation::new(delivery(owner, repo, delivery_id));
        let fences = [PostgresResourceFence::new(
            ResourceLockKey::provider_repository("github", owner, repo),
            61,
        )];
        let mut connection = pool.acquire().await.expect("connection");
        let outcome = super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
            &mut connection,
            &fences,
            &mutation,
        )
        .await
        .expect("stale outcome");
        assert_eq!(outcome, PostgresProviderMutationOutcome::StaleFence);

        let delivery_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2 AND delivery_id = $3",
        )
        .bind(owner)
        .bind(repo)
        .bind(delivery_id)
        .fetch_one(&pool)
        .await
        .expect("delivery count");
        assert_eq!(delivery_count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_mutation_rejects_tampered_delete_and_preserves_state() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner = "provider-mutation-tampered-delete";
        let repo = "repository";
        let resource = "github:provider-mutation-tampered-delete/repository";
        query(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean delivery fixture");
        query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean state fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(format!("github:{owner}:{repo}"))
        .execute(&pool)
        .await
        .expect("clean evidence fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery'
               AND (operation_id = $1 OR operation_id = $2)",
        )
        .bind(webhook_operation_id(&delivery(
            owner,
            repo,
            "delivery-tampered-seed",
        )))
        .bind("delivery-tampered-seed")
        .execute(&pool)
        .await
        .expect("clean seed delivery evidence fixture");
        set_fence(&pool, resource, 71).await;

        let mut seed =
            PostgresProviderMutation::new(delivery(owner, repo, "delivery-tampered-seed"));
        seed.upsert_provider_repository_state(ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            owner.to_owned(),
            repo.to_owned(),
            Some(100),
            None,
            None,
        ));
        let fences = [PostgresResourceFence::new(
            ResourceLockKey::provider_repository("github", owner, repo),
            71,
        )];
        let mut connection = pool.acquire().await.expect("seed connection");
        assert_eq!(
            super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
                &mut connection,
                &fences,
                &seed,
            )
            .await
            .expect("seed mutation"),
            PostgresProviderMutationOutcome::Applied
        );
        query(
            "UPDATE shardline_reliability_events
             SET event_json = '{\"sequence\": 99}'
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(format!("github:{owner}:{repo}"))
        .execute(&pool)
        .await
        .expect("tamper evidence");

        let mut deletion =
            PostgresProviderMutation::new(delivery(owner, repo, "delivery-tampered-delete"));
        deletion.delete_provider_repository_state(ProviderRepositoryKey::new(
            RepositoryProvider::GitHub,
            owner.to_owned(),
            repo.to_owned(),
        ));
        let result = super::super::PostgresIndexStore::commit_provider_mutation_on_connection(
            &mut connection,
            &fences,
            &deletion,
        )
        .await;
        assert!(result.is_err());

        let state_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .fetch_one(&pool)
        .await
        .expect("state count");
        let delivery_count = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_webhook_deliveries
             WHERE provider = 'github' AND owner = $1 AND repo = $2
               AND delivery_id = 'delivery-tampered-delete'",
        )
        .bind(owner)
        .bind(repo)
        .fetch_one(&pool)
        .await
        .expect("delivery count");
        assert_eq!(state_count, 1);
        assert_eq!(delivery_count, 0);
    }
}
