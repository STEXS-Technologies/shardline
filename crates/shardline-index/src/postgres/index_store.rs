use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_reliability::{
    EvidenceEventMetadata, LifecycleEvent, OperationKind, ProviderLifecycleEvent,
    QuarantineEvidenceLog, QuarantineLifecycleEvent, QuarantineLifecycleState,
    QuarantineObjectIdentity, QuarantineSnapshot, ReliabilityMerkleCommit, RetentionEvidenceLog,
    RetentionHoldLifecycleState, RetentionHoldSnapshot, RetentionObjectIdentity, SnapshotEvidence,
    WebhookDeliveryEvidenceLog, WebhookDeliveryIdentity, WebhookDeliveryLifecycleState,
    WebhookDeliverySnapshot, append_or_baseline_snapshot_evidence,
    baseline_upload_lifecycle_events, reliability_merkle_commit_json_with_previous,
    upload_lifecycle_event, upload_lifecycle_identity, verify_and_append_snapshot_transition,
    verify_persisted_event_merkle_chain, verify_persisted_event_merkle_chain_with_sequences,
    verify_provider_lifecycle_events, verify_snapshot_evidence, verify_upload_lifecycle_events,
};
use shardline_storage::ObjectKey;
use sqlx::{PgConnection, Row, postgres::PgRow, query, query_scalar, types::Json};

use super::{PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, IndexStoreFuture,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RepoKey, RetentionHold,
    StoredObjectId, TreeStore, WebhookDelivery, WebhookDeliveryError, parse_xet_hash_hex,
    provider::parse_repository_provider,
    upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore},
    xet_hash_hex_string,
};

async fn verify_postgres_intent_evidence(
    store: &super::PostgresIndexStore,
    intent: &crate::UploadIntent,
) -> Result<(), PostgresMetadataStoreError> {
    // The intent row and its terminal reliability event are committed in one
    // transition transaction, but these compatibility reads use separate pool
    // snapshots. A concurrent node can therefore advance both between the two
    // reads and briefly present a mixed pair to this verifier. Retry the
    // bounded read/verify operation; persistent corruption still returns after
    // the final attempt.
    const MAX_ATTEMPTS: usize = 3;
    let mut attempt = 0_usize;
    loop {
        let events = <super::PostgresIndexStore as UploadIntentStore>::reliability_events(
            store,
            intent.intent_id(),
        )
        .await?;
        let (tenant, repository) = upload_lifecycle_identity(&events);
        match verify_upload_lifecycle_events(
            &events,
            tenant,
            repository,
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            intent.state(),
        ) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let current_state = query_scalar::<_, String>(
                    "SELECT state FROM shardline_upload_intents WHERE intent_id = $1",
                )
                .bind(intent.intent_id())
                .fetch_optional(&store.pool)
                .await?;
                if current_state
                    .as_deref()
                    .and_then(UploadIntentState::parse)
                    .is_some_and(|state| state != intent.state())
                {
                    // The row advanced after this caller loaded its snapshot.
                    // Its CAS recovery path will reload the authoritative row;
                    // do not reject startup for a valid concurrent transition.
                    return Ok(());
                }
                let next_attempt = attempt.saturating_add(1);
                if next_attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        u64::try_from(next_attempt).unwrap_or(1),
                    ))
                    .await;
                    attempt = next_attempt;
                } else {
                    return Err(error.into());
                }
            }
        }
    }
}

async fn verify_postgres_provider_evidence(
    store: &super::PostgresIndexStore,
    state: &ProviderRepositoryState,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = crate::provider_evidence::snapshot_from_state(state)?;
    let operation_id = snapshot.evidence_operation()?.operation_id;
    let mut transaction = store.pool.begin().await?;
    let rows = query(
        "SELECT sequence, event_json, merkle_commit_json
         FROM shardline_reliability_events
         WHERE operation_kind = 'ProviderEvent' AND operation_id = $1
         ORDER BY sequence",
    )
    .bind(&operation_id)
    .fetch_all(&mut *transaction)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    let mut row_sequences = Vec::with_capacity(rows.len());
    let mut event_json = Vec::with_capacity(rows.len());
    let mut merkle_commits = Vec::with_capacity(rows.len());
    for row in rows {
        row_sequences.push(
            u64::try_from(row.try_get::<i64, _>("sequence")?).map_err(|_| {
                PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
            })?,
        );
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(serde_json::from_value::<ProviderLifecycleEvent>(
            value.clone(),
        )?);
        event_json.push(value);
        merkle_commits.push(row.try_get("merkle_commit_json")?);
    }
    verify_persisted_event_merkle_chain_with_sequences(
        OperationKind::ProviderEvent,
        &row_sequences,
        &event_json,
        &merkle_commits,
    )?;
    verify_provider_lifecycle_events(&events, &snapshot)?;
    transaction.commit().await?;
    Ok(())
}

async fn load_postgres_quarantine_evidence(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    object_key: &str,
) -> Result<QuarantineEvidenceLog, PostgresMetadataStoreError> {
    let rows = query(
        "SELECT sequence, event_json, merkle_commit_json FROM shardline_reliability_events
         WHERE operation_kind = 'GarbageCollection' AND operation_id = $1 ORDER BY sequence",
    )
    .bind(object_key)
    .fetch_all(executor)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    let mut row_sequences = Vec::with_capacity(rows.len());
    let mut event_json = Vec::with_capacity(rows.len());
    let mut merkle_commits = Vec::with_capacity(rows.len());
    for row in rows {
        row_sequences.push(
            u64::try_from(row.try_get::<i64, _>("sequence")?).map_err(|_| {
                PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
            })?,
        );
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(serde_json::from_value::<QuarantineLifecycleEvent>(
            value.clone(),
        )?);
        event_json.push(value);
        merkle_commits.push(row.try_get("merkle_commit_json")?);
    }
    verify_persisted_event_merkle_chain_with_sequences(
        OperationKind::GarbageCollection,
        &row_sequences,
        &event_json,
        &merkle_commits,
    )?;
    Ok(QuarantineEvidenceLog::from_events(events)?)
}

pub(super) async fn load_postgres_retention_evidence(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    object_key: &str,
) -> Result<RetentionEvidenceLog, PostgresMetadataStoreError> {
    let rows = query(
        "SELECT sequence, event_json, merkle_commit_json FROM shardline_reliability_events
         WHERE operation_kind = 'RetentionHold' AND operation_id = $1 ORDER BY sequence",
    )
    .bind(object_key)
    .fetch_all(executor)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    let mut row_sequences = Vec::with_capacity(rows.len());
    let mut event_json = Vec::with_capacity(rows.len());
    let mut merkle_commits = Vec::with_capacity(rows.len());
    for row in rows {
        row_sequences.push(
            u64::try_from(row.try_get::<i64, _>("sequence")?).map_err(|_| {
                PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
            })?,
        );
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(serde_json::from_value::<
            shardline_reliability::RetentionHoldLifecycleEvent,
        >(value.clone())?);
        event_json.push(value);
        merkle_commits.push(row.try_get("merkle_commit_json")?);
    }
    verify_persisted_event_merkle_chain_with_sequences(
        OperationKind::RetentionHold,
        &row_sequences,
        &event_json,
        &merkle_commits,
    )?;
    Ok(RetentionEvidenceLog::from_events(events)?)
}

fn quarantine_snapshot(
    candidate: &QuarantineCandidate,
    state: QuarantineLifecycleState,
) -> Result<QuarantineSnapshot, PostgresMetadataStoreError> {
    Ok(QuarantineSnapshot::new(
        QuarantineObjectIdentity::new(candidate.object_key().as_str())?,
        candidate.observed_length(),
        candidate.first_seen_unreachable_at_unix_seconds(),
        candidate.delete_after_unix_seconds(),
        state,
    )?)
}

pub(super) fn retention_snapshot(
    hold: &RetentionHold,
    state: RetentionHoldLifecycleState,
) -> Result<RetentionHoldSnapshot, PostgresMetadataStoreError> {
    Ok(RetentionHoldSnapshot::new(
        RetentionObjectIdentity::new(hold.object_key().as_str())?,
        hold.reason(),
        hold.held_at_unix_seconds(),
        hold.release_after_unix_seconds(),
        state,
    )?)
}

pub(super) fn webhook_snapshot(
    delivery: &WebhookDelivery,
    state: WebhookDeliveryLifecycleState,
) -> Result<WebhookDeliverySnapshot, PostgresMetadataStoreError> {
    Ok(WebhookDeliverySnapshot::new(
        WebhookDeliveryIdentity::new(
            delivery.provider().as_str(),
            delivery.owner(),
            delivery.repo(),
            delivery.delivery_id(),
        )?,
        delivery.processed_at_unix_seconds(),
        state,
    ))
}

pub(super) async fn load_postgres_webhook_evidence(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    delivery: &WebhookDelivery,
) -> Result<WebhookDeliveryEvidenceLog, PostgresMetadataStoreError> {
    let operation = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?
        .evidence_operation()?;
    let rows = query(
        "SELECT sequence, event_json, merkle_commit_json FROM shardline_reliability_events
         WHERE operation_kind = 'WebhookDelivery'
           AND (operation_id = $1 OR (operation_id = $2 AND NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1
           )))
         ORDER BY sequence",
    )
    .bind(&operation.operation_id)
    .bind(delivery.delivery_id())
    .fetch_all(executor)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    let mut row_sequences = Vec::with_capacity(rows.len());
    let mut event_json = Vec::with_capacity(rows.len());
    let mut merkle_commits = Vec::with_capacity(rows.len());
    for row in rows {
        row_sequences.push(
            u64::try_from(row.try_get::<i64, _>("sequence")?).map_err(|_| {
                PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
            })?,
        );
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(serde_json::from_value::<
            shardline_reliability::WebhookDeliveryLifecycleEvent,
        >(value.clone())?);
        event_json.push(value);
        merkle_commits.push(row.try_get("merkle_commit_json")?);
    }
    verify_persisted_event_merkle_chain_with_sequences(
        OperationKind::WebhookDelivery,
        &row_sequences,
        &event_json,
        &merkle_commits,
    )?;
    Ok(WebhookDeliveryEvidenceLog::from_events(events)?)
}

impl AsyncIndexStore for super::PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move {
            let row = query("SELECT terms FROM shardline_file_reconstructions WHERE file_id = $1")
                .bind(xet_hash_hex_string(file_id.hash()))
                .fetch_optional(&self.pool)
                .await?;

            let Some(row) = row else {
                return Ok(None);
            };
            let Json(record) = row.try_get::<Json<PostgresFileReconstructionRecord>, _>("terms")?;
            Ok(Some(record.into_domain()?))
        })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let record = PostgresFileReconstructionRecord::from_domain(reconstruction);
            query(
                "INSERT INTO shardline_file_reconstructions (file_id, terms)
                 VALUES ($1, $2)
                 ON CONFLICT (file_id)
                 DO UPDATE SET terms = EXCLUDED.terms, updated_at = now()",
            )
            .bind(xet_hash_hex_string(file_id.hash()))
            .bind(Json(record))
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move {
            let rows = query("SELECT file_id FROM shardline_file_reconstructions ORDER BY file_id")
                .fetch_all(&self.pool)
                .await?;

            rows.iter()
                .map(|row| {
                    let file_id = row.try_get::<String, _>("file_id")?;
                    let hash = parse_xet_hash_hex(&file_id)?;
                    Ok(FileId::new(hash))
                })
                .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()
        })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_file_reconstructions WHERE file_id = $1")
                .bind(xet_hash_hex_string(file_id.hash()))
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_reconstruction_if_matches<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        expected: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let record = PostgresFileReconstructionRecord::from_domain(expected);
            let result = query(
                "DELETE FROM shardline_file_reconstructions
                 WHERE file_id = $1 AND terms = $2",
            )
            .bind(xet_hash_hex_string(file_id.hash()))
            .bind(Json(record))
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn contains_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let exists = query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM shardline_stored_objects WHERE object_hash = $1
                 )",
            )
            .bind(xet_hash_hex_string(object_id.hash()))
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        })
    }

    fn insert_object<'operation>(
        &'operation self,
        object_id: &'operation StoredObjectId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_stored_objects (object_hash)
                 VALUES ($1)
                 ON CONFLICT (object_hash) DO NOTHING",
            )
            .bind(xet_hash_hex_string(object_id.hash()))
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = $1",
            )
            .bind(xet_hash_hex_string(chunk_hash))
            .fetch_optional(&self.pool)
            .await?;

            row.as_ref().map(dedupe_shard_mapping_from_row).transpose()
        })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 ORDER BY chunk_hash",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.iter()
                .map(dedupe_shard_mapping_from_row)
                .collect::<Result<Vec<_>, _>>()
        })
    }

    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 ORDER BY chunk_hash",
            )
            .fetch(&self.pool);

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                let mapping = dedupe_shard_mapping_from_row(&row).map_err(Into::into)?;
                visitor(mapping)?;
            }

            Ok(())
        })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            query(
                "INSERT INTO shardline_dedupe_shards (chunk_hash, shard_object_key)
                 VALUES ($1, $2)
                 ON CONFLICT (chunk_hash)
                 DO UPDATE SET
                    shard_object_key = EXCLUDED.shard_object_key,
                    updated_at = now()",
            )
            .bind(xet_hash_hex_string(mapping.chunk_hash()))
            .bind(mapping.shard_object_key().as_str())
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_dedupe_shards WHERE chunk_hash = $1")
                .bind(xet_hash_hex_string(chunk_hash))
                .execute(&self.pool)
                .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_dedupe_shard_mapping_if_matches<'operation>(
        &'operation self,
        expected: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let result = query(
                "DELETE FROM shardline_dedupe_shards
                 WHERE chunk_hash = $1 AND shard_object_key = $2",
            )
            .bind(xet_hash_hex_string(expected.chunk_hash()))
            .bind(expected.shard_object_key().as_str())
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&self.pool)
            .await?;

            let candidate = row
                .as_ref()
                .map(quarantine_candidate_from_row)
                .transpose()?;
            if let Some(candidate) = &candidate {
                let snapshot = quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
                let evidence =
                    load_postgres_quarantine_evidence(&self.pool, object_key.as_str()).await?;
                verify_snapshot_evidence(&evidence, &snapshot)?;
            }
            Ok(candidate)
        })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 ORDER BY object_key",
            )
            .fetch_all(&self.pool)
            .await?;

            let candidates = rows
                .iter()
                .map(quarantine_candidate_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            for candidate in &candidates {
                let snapshot = quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
                let evidence =
                    load_postgres_quarantine_evidence(&self.pool, candidate.object_key().as_str())
                        .await?;
                verify_snapshot_evidence(&evidence, &snapshot)?;
            }
            Ok(candidates)
        })
    }

    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 ORDER BY object_key",
            )
            .fetch(&self.pool);

            let mut candidates = Vec::new();
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                candidates.push(quarantine_candidate_from_row(&row).map_err(Into::into)?);
            }

            // This visitor feeds GC and repair directly. Verify every row at
            // this boundary instead of relying on callers to have used the
            // separately verified list API.
            for candidate in candidates {
                let snapshot = quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)
                    .map_err(Into::<VisitorError>::into)?;
                let evidence =
                    load_postgres_quarantine_evidence(&self.pool, candidate.object_key().as_str())
                        .await
                        .map_err(Into::<VisitorError>::into)?;
                verify_snapshot_evidence(&evidence, &snapshot)
                    .map_err(Self::Error::from)
                    .map_err(Into::<VisitorError>::into)?;
                visitor(candidate)?;
            }

            Ok(())
        })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let previous = query(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = $1 FOR UPDATE",
            )
            .bind(candidate.object_key().as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .as_ref()
            .map(quarantine_candidate_from_row)
            .transpose()?;
            query(
                "INSERT INTO shardline_quarantine_candidates (
                    object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
                 )
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (object_key)
                 DO UPDATE SET
                    observed_length = EXCLUDED.observed_length,
                    first_seen_unreachable_at_unix_seconds =
                        EXCLUDED.first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds = EXCLUDED.delete_after_unix_seconds",
            )
            .bind(candidate.object_key().as_str())
            .bind(u64_to_i64(candidate.observed_length())?)
            .bind(u64_to_i64(
                candidate.first_seen_unreachable_at_unix_seconds(),
            )?)
            .bind(u64_to_i64(candidate.delete_after_unix_seconds())?)
            .execute(&mut *transaction)
            .await?;
            let snapshot = quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
            let evidence = load_postgres_quarantine_evidence(
                &mut *transaction,
                candidate.object_key().as_str(),
            )
            .await?;
            let (evidence, evidence_was_empty) = if let Some(previous) = previous {
                let before = quarantine_snapshot(&previous, QuarantineLifecycleState::Active)?;
                verify_and_append_snapshot_transition(evidence, before, snapshot)?
            } else if evidence.events().is_empty() {
                (
                    append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                    true,
                )
            } else {
                let released = quarantine_snapshot(candidate, QuarantineLifecycleState::Released)?;
                verify_and_append_snapshot_transition(evidence, released, snapshot)?
            };
            let event = evidence.events().last().ok_or_else(|| {
                PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "quarantine evidence event",
                    ),
                )
            })?;
            if evidence_was_empty {
                for stored_event in evidence.events() {
                    insert_reliability_event(&mut *transaction, stored_event).await?;
                }
            } else {
                insert_reliability_event(&mut *transaction, event).await?;
            }
            transaction.commit().await?;
            Ok(())
        })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&mut *transaction)
            .await?;
            let candidate = row
                .as_ref()
                .map(quarantine_candidate_from_row)
                .transpose()?;
            let result = query("DELETE FROM shardline_quarantine_candidates WHERE object_key = $1")
                .bind(object_key.as_str())
                .execute(&mut *transaction)
                .await?;
            if let Some(candidate) = candidate {
                let active = quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
                let released = quarantine_snapshot(&candidate, QuarantineLifecycleState::Released)?;
                let evidence =
                    load_postgres_quarantine_evidence(&mut *transaction, object_key.as_str())
                        .await?;
                let (evidence, evidence_was_empty) =
                    verify_and_append_snapshot_transition(evidence, active, released)?;
                let event = evidence.events().last().ok_or_else(|| {
                    PostgresMetadataStoreError::Reliability(
                        shardline_reliability::ReliabilityError::EmptyField(
                            "quarantine evidence event",
                        ),
                    )
                })?;
                if evidence_was_empty {
                    for stored_event in evidence.events() {
                        insert_reliability_event(&mut *transaction, stored_event).await?;
                    }
                } else {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            }
            transaction.commit().await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_quarantine_candidate_if_matches<'operation>(
        &'operation self,
        expected: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = $1 FOR UPDATE",
            )
            .bind(expected.object_key().as_str())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                transaction.commit().await?;
                return Ok(false);
            };
            let candidate = quarantine_candidate_from_row(&row)?;
            if candidate != *expected {
                transaction.commit().await?;
                return Ok(false);
            }
            let active = quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
            let released = quarantine_snapshot(&candidate, QuarantineLifecycleState::Released)?;
            let evidence = load_postgres_quarantine_evidence(
                &mut *transaction,
                expected.object_key().as_str(),
            )
            .await?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let result = query(
                "DELETE FROM shardline_quarantine_candidates
                 WHERE object_key = $1 AND observed_length = $2
                   AND first_seen_unreachable_at_unix_seconds = $3
                   AND delete_after_unix_seconds = $4",
            )
            .bind(expected.object_key().as_str())
            .bind(u64_to_i64(expected.observed_length())?)
            .bind(u64_to_i64(
                expected.first_seen_unreachable_at_unix_seconds(),
            )?)
            .bind(u64_to_i64(expected.delete_after_unix_seconds())?)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                transaction.commit().await?;
                return Ok(false);
            }
            let event = evidence.events().last().ok_or_else(|| {
                PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "quarantine evidence event",
                    ),
                )
            })?;
            if evidence_was_empty {
                for stored_event in evidence.events() {
                    insert_reliability_event(&mut *transaction, stored_event).await?;
                }
            } else {
                insert_reliability_event(&mut *transaction, event).await?;
            }
            transaction.commit().await?;
            Ok(true)
        })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move {
            let row = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&self.pool)
            .await?;

            let hold = row.as_ref().map(retention_hold_from_row).transpose()?;
            if let Some(hold) = hold.as_ref() {
                let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
                let evidence =
                    load_postgres_retention_evidence(&self.pool, object_key.as_str()).await?;
                verify_snapshot_evidence(&evidence, &snapshot)?;
            }
            Ok(hold)
        })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 ORDER BY object_key",
            )
            .fetch_all(&self.pool)
            .await?;

            let holds = rows
                .iter()
                .map(retention_hold_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            for hold in &holds {
                let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
                let evidence =
                    load_postgres_retention_evidence(&self.pool, hold.object_key().as_str())
                        .await?;
                verify_snapshot_evidence(&evidence, &snapshot)?;
            }
            Ok(holds)
        })
    }

    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            let mut rows = query(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 ORDER BY object_key",
            )
            .fetch(&self.pool);

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(Self::Error::from)
                .map_err(Into::<VisitorError>::into)?
            {
                let hold = retention_hold_from_row(&row).map_err(Into::into)?;
                let snapshot = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)
                    .map_err(Into::into)?;
                let evidence =
                    load_postgres_retention_evidence(&self.pool, hold.object_key().as_str())
                        .await
                        .map_err(Into::into)?;
                verify_snapshot_evidence(&evidence, &snapshot)
                    .map_err(Self::Error::from)
                    .map_err(Into::<VisitorError>::into)?;
                visitor(hold)?;
            }

            Ok(())
        })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            super::provider_mutation::upsert_retention_hold(&mut transaction, hold).await?;
            transaction.commit().await?;
            Ok(())
        })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = $1",
            )
            .bind(object_key.as_str())
            .fetch_optional(&mut *transaction)
            .await?;
            let hold = row.as_ref().map(retention_hold_from_row).transpose()?;
            let result = query("DELETE FROM shardline_retention_holds WHERE object_key = $1")
                .bind(object_key.as_str())
                .execute(&mut *transaction)
                .await?;
            if let Some(hold) = hold {
                let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
                let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
                let evidence =
                    load_postgres_retention_evidence(&mut *transaction, object_key.as_str())
                        .await?;
                let (evidence, evidence_was_empty) =
                    verify_and_append_snapshot_transition(evidence, active, released)?;
                if evidence_was_empty {
                    for event in evidence.events() {
                        insert_reliability_event(&mut *transaction, event).await?;
                    }
                } else if let Some(event) = evidence.events().last() {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            }
            transaction.commit().await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_retention_hold_if_matches<'operation>(
        &'operation self,
        expected: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = $1 FOR UPDATE",
            )
            .bind(expected.object_key().as_str())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                transaction.commit().await?;
                return Ok(false);
            };
            let hold = retention_hold_from_row(&row)?;
            if hold != *expected {
                transaction.commit().await?;
                return Ok(false);
            }
            let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
            let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
            let evidence =
                load_postgres_retention_evidence(&mut *transaction, expected.object_key().as_str())
                    .await?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let result = query(
                "DELETE FROM shardline_retention_holds
                 WHERE object_key = $1 AND reason = $2 AND held_at_unix_seconds = $3
                   AND release_after_unix_seconds IS NOT DISTINCT FROM $4",
            )
            .bind(expected.object_key().as_str())
            .bind(expected.reason())
            .bind(u64_to_i64(expected.held_at_unix_seconds())?)
            .bind(
                expected
                    .release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            )
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                transaction.commit().await?;
                return Ok(false);
            }
            if evidence_was_empty {
                for event in evidence.events() {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            } else if let Some(event) = evidence.events().last() {
                insert_reliability_event(&mut *transaction, event).await?;
            }
            transaction.commit().await?;
            Ok(true)
        })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let recorded =
                super::provider_mutation::record_webhook_delivery(&mut transaction, delivery)
                    .await?;
            transaction.commit().await?;
            Ok(recorded)
        })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move {
            let rows = query(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 ORDER BY provider, owner, repo, delivery_id",
            )
            .fetch_all(&self.pool)
            .await?;
            let deliveries = rows
                .into_iter()
                .map(|row| webhook_delivery_from_row(&row))
                .collect::<Result<Vec<_>, _>>()?;
            for delivery in &deliveries {
                let snapshot =
                    webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
                let evidence = load_postgres_webhook_evidence(&self.pool, delivery).await?;
                verify_snapshot_evidence(&evidence, &snapshot)?;
            }
            Ok(deliveries)
        })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4
                 FOR UPDATE",
            )
            .bind(delivery.provider().as_str())
            .bind(delivery.owner())
            .bind(delivery.repo())
            .bind(delivery.delivery_id())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                transaction.commit().await?;
                return Ok(false);
            };
            let existing = webhook_delivery_from_row(&row)?;
            let active = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
            let evidence = load_postgres_webhook_evidence(&mut *transaction, &existing).await?;
            let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let result = query(
                "DELETE FROM shardline_webhook_deliveries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4",
            )
            .bind(existing.provider().as_str())
            .bind(existing.owner())
            .bind(existing.repo())
            .bind(existing.delivery_id())
            .execute(&mut *transaction)
            .await?;
            if evidence_was_empty {
                for event in evidence.events() {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            } else if let Some(event) = evidence.events().last() {
                insert_reliability_event(&mut *transaction, event).await?;
            }
            transaction.commit().await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn delete_webhook_delivery_if_matches<'operation>(
        &'operation self,
        expected: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let row = query(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4
                 FOR UPDATE",
            )
            .bind(expected.provider().as_str())
            .bind(expected.owner())
            .bind(expected.repo())
            .bind(expected.delivery_id())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                transaction.commit().await?;
                return Ok(false);
            };
            let existing = webhook_delivery_from_row(&row)?;
            if existing != *expected {
                transaction.commit().await?;
                return Ok(false);
            }
            let active = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
            let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
            let evidence = load_postgres_webhook_evidence(&mut *transaction, &existing).await?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let result = query(
                "DELETE FROM shardline_webhook_deliveries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4
                   AND processed_at_unix_seconds = $5",
            )
            .bind(expected.provider().as_str())
            .bind(expected.owner())
            .bind(expected.repo())
            .bind(expected.delivery_id())
            .bind(u64_to_i64(expected.processed_at_unix_seconds())?)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() == 0 {
                transaction.commit().await?;
                return Ok(false);
            }
            if evidence_was_empty {
                for event in evidence.events() {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            } else if let Some(event) = evidence.events().last() {
                insert_reliability_event(&mut *transaction, event).await?;
            }
            transaction.commit().await?;
            Ok(true)
        })
    }

    fn purge_webhook_deliveries_older_than<'operation>(
        &'operation self,
        older_than_unix_seconds: u64,
    ) -> IndexStoreFuture<'operation, u64, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let rows = query(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE processed_at_unix_seconds < $1
                 FOR UPDATE",
            )
            .bind(u64_to_i64(older_than_unix_seconds)?)
            .fetch_all(&mut *transaction)
            .await?;
            let deliveries = rows
                .iter()
                .map(webhook_delivery_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            for delivery in &deliveries {
                let active = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
                let evidence = load_postgres_webhook_evidence(&mut *transaction, delivery).await?;
                let released = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Released)?;
                let (evidence, evidence_was_empty) =
                    verify_and_append_snapshot_transition(evidence, active, released)?;
                query(
                    "DELETE FROM shardline_webhook_deliveries
                     WHERE provider = $1 AND owner = $2 AND repo = $3 AND delivery_id = $4",
                )
                .bind(delivery.provider().as_str())
                .bind(delivery.owner())
                .bind(delivery.repo())
                .bind(delivery.delivery_id())
                .execute(&mut *transaction)
                .await?;
                if evidence_was_empty {
                    for event in evidence.events() {
                        insert_reliability_event(&mut *transaction, event).await?;
                    }
                } else if let Some(event) = evidence.events().last() {
                    insert_reliability_event(&mut *transaction, event).await?;
                }
            }
            transaction.commit().await?;
            Ok(u64::try_from(deliveries.len()).unwrap_or(u64::MAX))
        })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
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
            .bind(provider.as_str())
            .bind(owner)
            .bind(repo)
            .fetch_optional(&self.pool)
            .await?;

            let state = row
                .as_ref()
                .map(provider_repository_state_from_row)
                .transpose()?;
            if let Some(state) = state.as_ref() {
                verify_postgres_provider_evidence(self, state).await?;
            }
            Ok(state)
        })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
            let rows = query(
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
                 ORDER BY provider, owner, repo",
            )
            .fetch_all(&self.pool)
            .await?;
            let states = rows
                .into_iter()
                .map(|row| provider_repository_state_from_row(&row))
                .collect::<Result<Vec<_>, _>>()?;
            for state in &states {
                verify_postgres_provider_evidence(self, state).await?;
            }
            Ok(states)
        })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            super::provider_mutation::upsert_provider_repository_state(&mut transaction, state)
                .await?;
            transaction.commit().await?;
            Ok(())
        })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await?;
            let operation_id = shardline_reliability::ProviderRepositoryOperationId::new(
                provider.as_str(),
                owner,
                repo,
            );
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
            .bind(provider.as_str())
            .bind(owner)
            .bind(repo)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = current {
                let state = provider_repository_state_from_row(&row)?;
                super::provider_mutation::verify_provider_repository_state_evidence(
                    &mut transaction,
                    &state,
                )
                .await?;
            }
            let result = query(
                "DELETE FROM shardline_provider_repository_states
                 WHERE provider = $1 AND owner = $2 AND repo = $3",
            )
            .bind(provider.as_str())
            .bind(owner)
            .bind(repo)
            .execute(&mut *transaction)
            .await?;
            query(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
            )
            .bind(operation_id.as_str())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn prune_revisions_over_cap<'operation>(
        &'operation self,
        key: &'operation RepoKey,
        max_revisions: usize,
    ) -> IndexStoreFuture<'operation, u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        Box::pin(
            async move { TreeStore::prune_revisions_over_cap(&store, &key, max_revisions).await },
        )
    }

    fn list_revision_repo_keys(&self) -> IndexStoreFuture<'_, Vec<RepoKey>, Self::Error> {
        let store = self.clone();
        Box::pin(async move { TreeStore::list_revision_repo_keys(&store).await })
    }
}

#[async_trait::async_trait]
impl UploadIntentStore for super::PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error> {
        self.create_intent_scoped(intent, "shardline", "default")
            .await
    }

    async fn create_intent_scoped(
        &self,
        intent: &UploadIntent,
        tenant: &str,
        repository: &str,
    ) -> Result<(), Self::Error> {
        let created_event = upload_lifecycle_event(
            tenant,
            repository,
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )?;
        let mut transaction = self.pool.begin().await?;
        query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(intent.intent_id())
            .execute(&mut *transaction)
            .await?;
        let existing = query(
            "SELECT intent_id, object_key, object_hash, object_length, state,
                    created_at, updated_at
             FROM shardline_upload_intents WHERE intent_id = $1 FOR UPDATE",
        )
        .bind(intent.intent_id())
        .fetch_optional(&mut *transaction)
        .await?;
        let result = sqlx::query(
            "INSERT INTO shardline_upload_intents (
                intent_id, object_key, object_hash, object_length, state, created_at, updated_at
             )
             VALUES ($1, $2, $3, $4, $5, now(), now())
             ON CONFLICT (intent_id) DO UPDATE SET intent_id = EXCLUDED.intent_id
             WHERE shardline_upload_intents.object_key = EXCLUDED.object_key
               AND shardline_upload_intents.object_hash = EXCLUDED.object_hash
               AND shardline_upload_intents.object_length = EXCLUDED.object_length",
        )
        .bind(intent.intent_id())
        .bind(intent.object_key())
        .bind(intent.object_hash())
        .bind(intent.object_length() as i64)
        .bind(intent.state().as_str())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Err(crate::UploadIntentConflictError::new(intent.intent_id()).into());
        }
        if let Some(existing) = existing {
            let state_text: String = existing.try_get("state")?;
            let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                PostgresMetadataStoreError::InvalidUploadIntentState(state_text.clone())
            })?;
            let durable_intent = UploadIntent::from_parts(
                existing.try_get("intent_id")?,
                existing.try_get("object_key")?,
                existing.try_get("object_hash")?,
                i64_to_u64(existing.try_get("object_length")?)?,
                state,
                std::time::Duration::from_secs(
                    existing
                        .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")?
                        .timestamp() as u64,
                ),
                std::time::Duration::from_secs(
                    existing
                        .try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")?
                        .timestamp() as u64,
                ),
            );
            let event_rows = query(
                "SELECT event_json, merkle_commit_json FROM shardline_reliability_events
                 WHERE operation_kind = 'Upload' AND operation_id = $1
                 ORDER BY sequence",
            )
            .bind(intent.intent_id())
            .fetch_all(&mut *transaction)
            .await?;
            if event_rows.is_empty() {
                let baseline = baseline_upload_lifecycle_events(
                    tenant,
                    repository,
                    durable_intent.intent_id(),
                    durable_intent.object_key(),
                    durable_intent.object_hash(),
                    durable_intent.state(),
                )?;
                for event in &baseline {
                    insert_reliability_event(transaction.as_mut(), event).await?;
                }
            } else {
                let mut events = Vec::with_capacity(event_rows.len());
                let mut event_json = Vec::with_capacity(event_rows.len());
                let mut merkle_commits = Vec::with_capacity(event_rows.len());
                for row in event_rows {
                    let value: serde_json::Value = row.try_get("event_json")?;
                    events.push(serde_json::from_value::<LifecycleEvent>(value.clone())?);
                    event_json.push(value);
                    merkle_commits.push(row.try_get("merkle_commit_json")?);
                }
                let merkle_complete = merkle_commits.iter().all(Option::is_some);
                if merkle_complete {
                    verify_persisted_event_merkle_chain(
                        OperationKind::Upload,
                        &event_json,
                        &merkle_commits,
                    )?;
                }
                let (stored_tenant, stored_repository) = upload_lifecycle_identity(&events);
                if stored_tenant != tenant || stored_repository != repository {
                    // Upload evidence predating repository-scoped identities was
                    // written with the compatibility `default` repository. The
                    // durable intent and its object identity are unchanged, so
                    // migrate only the evidence identity while preserving the
                    // authoritative lifecycle state. Invalid chains still fail
                    // closed instead of being silently repaired.
                    verify_upload_lifecycle_events(
                        &events,
                        stored_tenant,
                        stored_repository,
                        durable_intent.intent_id(),
                        durable_intent.object_key(),
                        durable_intent.object_hash(),
                        durable_intent.state(),
                    )?;
                    sqlx::query(
                        "DELETE FROM shardline_reliability_events
                         WHERE operation_kind = 'Upload' AND operation_id = $1",
                    )
                    .bind(durable_intent.intent_id())
                    .execute(&mut *transaction)
                    .await?;
                    let migrated = baseline_upload_lifecycle_events(
                        tenant,
                        repository,
                        durable_intent.intent_id(),
                        durable_intent.object_key(),
                        durable_intent.object_hash(),
                        durable_intent.state(),
                    )?;
                    for event in &migrated {
                        insert_reliability_event(transaction.as_mut(), event).await?;
                    }
                } else {
                    verify_upload_lifecycle_events(
                        &events,
                        tenant,
                        repository,
                        durable_intent.intent_id(),
                        durable_intent.object_key(),
                        durable_intent.object_hash(),
                        durable_intent.state(),
                    )?;
                    if !merkle_complete {
                        // Evidence written before the Merkle column existed is
                        // still authoritative after its typed lifecycle has
                        // been validated. Reuse the canonical insert boundary
                        // to fill only missing commitments, then verify the
                        // complete persisted chain before committing.
                        for event in &events {
                            insert_reliability_event(transaction.as_mut(), event).await?;
                        }
                        let repaired_rows = query(
                            "SELECT event_json, merkle_commit_json
                             FROM shardline_reliability_events
                             WHERE operation_kind = 'Upload' AND operation_id = $1
                             ORDER BY sequence",
                        )
                        .bind(intent.intent_id())
                        .fetch_all(&mut *transaction)
                        .await?;
                        let mut repaired_events = Vec::with_capacity(repaired_rows.len());
                        let mut repaired_commits = Vec::with_capacity(repaired_rows.len());
                        for row in repaired_rows {
                            let value: serde_json::Value = row.try_get("event_json")?;
                            repaired_events.push(value.clone());
                            repaired_commits.push(row.try_get("merkle_commit_json")?);
                        }
                        verify_persisted_event_merkle_chain(
                            OperationKind::Upload,
                            &repaired_events,
                            &repaired_commits,
                        )?;
                    }
                }
            }
        } else {
            // A previously deleted materialized row may leave legacy evidence
            // behind. A newly created intent with the same identity starts a
            // new lifecycle, so discard only that orphaned Upload evidence
            // before its baseline event.
            sqlx::query(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'Upload' AND operation_id = $1",
            )
            .bind(intent.intent_id())
            .execute(&mut *transaction)
            .await?;
            insert_reliability_event(transaction.as_mut(), &created_event).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn transition_intent(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
    ) -> Result<bool, Self::Error> {
        let current = self.intent_by_id(intent_id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.state() == new_state {
            // Idempotent: already in the target state (concurrent duplicate
            // caller performing the same transition).
            return Ok(true);
        }
        if !current.state().can_transition_to(new_state) {
            return Ok(false);
        }
        let events = self.reliability_events(intent_id).await?;
        let (tenant, repository) = upload_lifecycle_identity(&events);
        let event = upload_lifecycle_event(
            tenant,
            repository,
            current.intent_id(),
            current.object_key(),
            current.object_hash(),
            current.state(),
            new_state,
        )?;
        if self
            .transition_intent_with_event(intent_id, new_state, &event)
            .await?
        {
            return Ok(true);
        }
        Ok(self
            .intent_by_id(intent_id)
            .await?
            .is_some_and(|intent| intent.state() == new_state))
    }

    async fn transition_intent_with_event(
        &self,
        intent_id: &str,
        new_state: UploadIntentState,
        event: &LifecycleEvent,
    ) -> Result<bool, Self::Error> {
        let current = self.intent_by_id(intent_id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.state() == new_state {
            return Ok(true);
        }
        if !current.state().can_transition_to(new_state) {
            return Ok(false);
        }
        event.validate_for_transition(intent_id, current.state(), new_state)?;
        let mut transaction = self.pool.begin().await?;
        let rows = sqlx::query(
            "UPDATE shardline_upload_intents SET state = $1, updated_at = now()
             WHERE intent_id = $2 AND state = $3",
        )
        .bind(new_state.as_str())
        .bind(intent_id)
        .bind(current.state().as_str())
        .execute(&mut *transaction)
        .await?;
        if rows.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }
        insert_reliability_event(transaction.as_mut(), event).await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn intent_by_id(&self, intent_id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        let row = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE intent_id = $1"
        )
        .bind(intent_id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((id, key, hash, length, state_str, created, updated)) => {
                let state = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                let created_dur = std::time::Duration::from_secs(created.timestamp() as u64);
                let updated_dur = std::time::Duration::from_secs(updated.timestamp() as u64);
                let intent = UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    state,
                    created_dur,
                    updated_dur,
                );
                verify_postgres_intent_evidence(self, &intent).await?;
                Ok(Some(intent))
            }
            None => Ok(None),
        }
    }

    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let rows = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE state = $1 ORDER BY created_at"
        )
        .bind(state.as_str())
        .fetch_all(&self.pool)
        .await?;
        let intents = rows
            .into_iter()
            .map(|(id, key, hash, length, state_str, created, updated)| {
                let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                Ok(UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    s,
                    std::time::Duration::from_secs(created.timestamp() as u64),
                    std::time::Duration::from_secs(updated.timestamp() as u64),
                ))
            })
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        for intent in &intents {
            verify_postgres_intent_evidence(self, intent).await?;
        }
        Ok(intents)
    }

    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: std::time::Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let duration = chrono::Duration::from_std(older_than).map_err(|_e| {
            PostgresMetadataStoreError::InvalidUploadIntentState("invalid duration".into())
        })?;
        let cutoff = chrono::Utc::now()
            .checked_sub_signed(duration)
            .ok_or_else(|| {
                PostgresMetadataStoreError::InvalidUploadIntentState("invalid duration".into())
            })?;
        let rows = sqlx::query_as::<_, (String, String, String, i64, String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT intent_id, object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE state = $1 AND created_at < $2 ORDER BY created_at"
        )
        .bind(state.as_str())
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        let intents = rows
            .into_iter()
            .map(|(id, key, hash, length, state_str, created, updated)| {
                let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    PostgresMetadataStoreError::InvalidUploadIntentState(state_str.clone())
                })?;
                Ok(UploadIntent::from_parts(
                    id,
                    key,
                    hash,
                    length as u64,
                    s,
                    std::time::Duration::from_secs(created.timestamp() as u64),
                    std::time::Duration::from_secs(updated.timestamp() as u64),
                ))
            })
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        for intent in &intents {
            verify_postgres_intent_evidence(self, intent).await?;
        }
        Ok(intents)
    }

    async fn record_reliability_event(&self, event: &LifecycleEvent) -> Result<(), Self::Error> {
        event.verify_integrity()?;
        let mut transaction = self.pool.begin().await?;
        let owner = sqlx::query(
            "SELECT object_key, object_hash, state
             FROM shardline_upload_intents
             WHERE intent_id = $1
             FOR UPDATE",
        )
        .bind(&event.operation.operation_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::EmptyField(
                    "reliability event has no authoritative upload intent",
                ),
            )
        })?;
        let object_key: String = owner.try_get("object_key")?;
        let object_hash: String = owner.try_get("object_hash")?;
        let state_text: String = owner.try_get("state")?;
        let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
            PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::EmptyField("unknown upload intent state"),
            )
        })?;
        let rows = sqlx::query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
             ORDER BY sequence",
        )
        .bind("Upload")
        .bind(&event.operation.operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let mut events = rows
            .into_iter()
            .map(
                |row| -> Result<LifecycleEvent, PostgresMetadataStoreError> {
                    Ok(serde_json::from_value(row.try_get("event_json")?)?)
                },
            )
            .collect::<Result<Vec<LifecycleEvent>, PostgresMetadataStoreError>>()?;
        if let Some(existing) = events
            .iter()
            .find(|existing| existing.sequence == event.sequence)
        {
            if existing != event {
                return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
                    event.operation.operation_id.clone(),
                ));
            }
        } else {
            events.push(event.clone());
            events.sort_by_key(|stored_event| stored_event.sequence);
        }
        let (tenant, repository) = upload_lifecycle_identity(&events);
        verify_upload_lifecycle_events(
            &events,
            tenant,
            repository,
            &event.operation.operation_id,
            &object_key,
            &object_hash,
            state,
        )?;
        insert_reliability_event(transaction.as_mut(), event).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn reliability_events(
        &self,
        operation_id: &str,
    ) -> Result<Vec<LifecycleEvent>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            "SELECT sequence, event_json, merkle_commit_json
             FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
             ORDER BY sequence",
        )
        .bind("Upload")
        .bind(operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        let mut event_json = Vec::with_capacity(rows.len());
        let mut merkle_commits = Vec::with_capacity(rows.len());
        for row in rows {
            let sequence: i64 = row.try_get("sequence")?;
            if sequence < 0 {
                return Err(PostgresMetadataStoreError::IntegerOutOfRange(
                    "reliability sequence".into(),
                ));
            }
            let value: serde_json::Value = row.try_get("event_json")?;
            let event = serde_json::from_value::<LifecycleEvent>(value.clone())?;
            if u64_to_i64(event.sequence)? != sequence {
                return Err(PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::Merkle(
                        "upload event sequence does not match its row".into(),
                    ),
                ));
            }
            events.push(event);
            event_json.push(value);
            merkle_commits.push(row.try_get("merkle_commit_json")?);
        }
        verify_persisted_event_merkle_chain(OperationKind::Upload, &event_json, &merkle_commits)?;
        let intent = sqlx::query(
            "SELECT object_key, object_hash, state
             FROM shardline_upload_intents
             WHERE intent_id = $1",
        )
        .bind(operation_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(intent) = intent {
            let object_key: String = intent.try_get("object_key")?;
            let object_hash: String = intent.try_get("object_hash")?;
            let state_text: String = intent.try_get("state")?;
            let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "unknown upload intent state",
                    ),
                )
            })?;
            let (tenant, repository) = upload_lifecycle_identity(&events);
            verify_upload_lifecycle_events(
                &events,
                tenant,
                repository,
                operation_id,
                &object_key,
                &object_hash,
                state,
            )?;
        } else {
            shardline_reliability::verify_lifecycle_chain(&events)?;
        }
        transaction.commit().await?;
        Ok(events)
    }
}

pub(crate) async fn insert_reliability_event<T>(
    connection: &mut PgConnection,
    event: &T,
) -> Result<(), PostgresMetadataStoreError>
where
    T: EvidenceEventMetadata,
{
    event.verify_integrity()?;
    let sequence = i64::try_from(event.sequence_number()).map_err(|_error| {
        PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
    })?;
    let previous_json: Option<serde_json::Value> = query_scalar(
        "SELECT merkle_commit_json
         FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2 AND sequence < $3
               AND merkle_commit_json IS NOT NULL
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(event.operation_identity().kind.as_str())
    .bind(&event.operation_identity().operation_id)
    .bind(sequence)
    .fetch_optional(&mut *connection)
    .await?;
    let previous = previous_json
        .map(serde_json::from_value::<ReliabilityMerkleCommit>)
        .transpose()?;
    let merkle_commit_json =
        reliability_merkle_commit_json_with_previous(event, previous.as_ref())?;
    insert_reliability_event_value(
        connection,
        event.operation_identity(),
        event.sequence_number(),
        serde_json::to_value(event)?,
        merkle_commit_json,
    )
    .await
}

async fn insert_reliability_event_value(
    connection: &mut PgConnection,
    operation: &shardline_reliability::OperationIdentity,
    event_sequence: u64,
    event_json: serde_json::Value,
    merkle_commit_json: serde_json::Value,
) -> Result<(), PostgresMetadataStoreError>
where
{
    let sequence = i64::try_from(event_sequence).map_err(|_error| {
        PostgresMetadataStoreError::IntegerOutOfRange("reliability sequence".into())
    })?;
    let row = sqlx::query(
        "INSERT INTO shardline_reliability_events
            (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds,
             merkle_commit_json)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (operation_kind, operation_id, sequence) DO UPDATE
         SET event_json = shardline_reliability_events.event_json,
             merkle_commit_json = COALESCE(
                 shardline_reliability_events.merkle_commit_json,
                 EXCLUDED.merkle_commit_json
             )
         WHERE shardline_reliability_events.event_json = EXCLUDED.event_json
         RETURNING event_json",
    )
    .bind(operation.kind.as_str())
    .bind(&operation.operation_id)
    .bind(sequence)
    .bind(event_json)
    .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
    .bind(merkle_commit_json)
    .fetch_optional(&mut *connection)
    .await?;
    if row.is_none() {
        return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
            operation.operation_id.clone(),
        ));
    }
    Ok(())
}

pub(crate) async fn next_reliability_sequence<'executor, E>(
    executor: E,
    operation_kind: shardline_reliability::OperationKind,
    operation_id: &str,
) -> Result<u64, PostgresMetadataStoreError>
where
    E: sqlx::Executor<'executor, Database = sqlx::Postgres>,
{
    let sequence: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence), 0) + 1
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .fetch_one(executor)
    .await?;
    i64_to_u64(sequence)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PostgresFileReconstructionRecord {
    terms: Vec<PostgresReconstructionTermRecord>,
}

impl PostgresFileReconstructionRecord {
    pub fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(PostgresReconstructionTermRecord::from_domain)
                .collect::<Vec<_>>(),
        }
    }

    pub fn into_domain(self) -> Result<FileReconstruction, PostgresMetadataStoreError> {
        let terms = self
            .terms
            .into_iter()
            .map(PostgresReconstructionTermRecord::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileReconstruction::new(terms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PostgresReconstructionTermRecord {
    object_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl PostgresReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            object_hash: xet_hash_hex_string(term.object_id().hash()),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, PostgresMetadataStoreError> {
        let hash = parse_xet_hash_hex(&self.object_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            StoredObjectId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

fn quarantine_candidate_from_row(
    row: &PgRow,
) -> Result<QuarantineCandidate, PostgresMetadataStoreError> {
    let object_key = ObjectKey::parse(row.try_get::<String, _>("object_key")?.as_str())?;
    let observed_length = i64_to_u64(row.try_get::<i64, _>("observed_length")?)?;
    let first_seen = i64_to_u64(row.try_get::<i64, _>("first_seen_unreachable_at_unix_seconds")?)?;
    let delete_after = i64_to_u64(row.try_get::<i64, _>("delete_after_unix_seconds")?)?;
    QuarantineCandidate::new(object_key, observed_length, first_seen, delete_after)
        .map_err(PostgresMetadataStoreError::from)
}

fn dedupe_shard_mapping_from_row(
    row: &PgRow,
) -> Result<DedupeShardMapping, PostgresMetadataStoreError> {
    let chunk_hash = parse_xet_hash_hex(row.try_get::<String, _>("chunk_hash")?.as_str())?;
    let shard_object_key =
        ObjectKey::parse(row.try_get::<String, _>("shard_object_key")?.as_str())?;
    Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
}

pub(super) fn retention_hold_from_row(
    row: &PgRow,
) -> Result<RetentionHold, PostgresMetadataStoreError> {
    let object_key = ObjectKey::parse(row.try_get::<String, _>("object_key")?.as_str())?;
    let reason = row.try_get::<String, _>("reason")?;
    let held_at_unix_seconds = i64_to_u64(row.try_get::<i64, _>("held_at_unix_seconds")?)?;
    let release_after_unix_seconds = row
        .try_get::<Option<i64>, _>("release_after_unix_seconds")?
        .map(i64_to_u64)
        .transpose()?;
    RetentionHold::new(
        object_key,
        reason,
        held_at_unix_seconds,
        release_after_unix_seconds,
    )
    .map_err(PostgresMetadataStoreError::from)
}

pub(super) fn webhook_delivery_from_row(
    row: &PgRow,
) -> Result<WebhookDelivery, PostgresMetadataStoreError> {
    let provider_name = row.try_get::<String, _>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
        PostgresMetadataStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
    })?;
    let owner = row.try_get::<String, _>("owner")?;
    let repo = row.try_get::<String, _>("repo")?;
    let delivery_id = row.try_get::<String, _>("delivery_id")?;
    let processed_at_unix_seconds =
        i64_to_u64(row.try_get::<i64, _>("processed_at_unix_seconds")?)?;
    WebhookDelivery::new(
        provider,
        owner,
        repo,
        delivery_id,
        processed_at_unix_seconds,
    )
    .map_err(PostgresMetadataStoreError::from)
}

pub(super) fn provider_repository_state_from_row(
    row: &PgRow,
) -> Result<ProviderRepositoryState, PostgresMetadataStoreError> {
    let provider_name = row.try_get::<String, _>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
        PostgresMetadataStoreError::InvalidRepoType(provider_name.clone())
    })?;
    Ok(ProviderRepositoryState::new(
        provider,
        row.try_get("owner")?,
        row.try_get("repo")?,
        row.try_get::<Option<i64>, _>("last_access_changed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_revision_pushed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get("last_pushed_revision")?,
    )
    .with_reconciliation(
        row.try_get::<Option<i64>, _>("last_cache_invalidated_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_authorization_rechecked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
        row.try_get::<Option<i64>, _>("last_drift_checked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()?,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use std::{
        io::{Read, Write},
        net::{Shutdown, TcpListener, TcpStream},
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    };

    use shardline_protocol::{ChunkRange, HashParseError, RepositoryProvider, ShardlineHash};
    use shardline_reliability::baseline_upload_lifecycle_events;
    use sqlx::{
        Row,
        postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    };

    use super::{PostgresFileReconstructionRecord, PostgresReconstructionTermRecord};
    use crate::{
        AsyncIndexStore, FileReconstruction, ProviderRepositoryState, QuarantineCandidate,
        ReconstructionTerm, StoredObjectId,
    };

    struct CommitResponseLossProxy {
        port: u16,
        response_dropped: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl CommitResponseLossProxy {
        fn start(upstream: String) -> std::io::Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            let response_dropped = Arc::new(AtomicBool::new(false));
            let worker_response_dropped = Arc::clone(&response_dropped);
            let thread = thread::spawn(move || {
                let Ok((mut client, _address)) = listener.accept() else {
                    return;
                };
                let Ok(mut provider) = TcpStream::connect(upstream) else {
                    return;
                };
                let Ok(mut client_requests) = client.try_clone() else {
                    return;
                };
                let Ok(mut provider_requests) = provider.try_clone() else {
                    return;
                };
                let request_thread = thread::spawn(move || {
                    std::io::copy(&mut client_requests, &mut provider_requests).ok();
                });
                forward_postgres_responses(&mut provider, &mut client, &worker_response_dropped)
                    .ok();
                client.shutdown(Shutdown::Both).ok();
                provider.shutdown(Shutdown::Both).ok();
                request_thread.join().ok();
            });
            Ok(Self {
                port,
                response_dropped,
                thread: Some(thread),
            })
        }

        const fn port(&self) -> u16 {
            self.port
        }

        fn response_was_dropped(&self) -> bool {
            self.response_dropped.load(Ordering::Acquire)
        }
    }

    impl Drop for CommitResponseLossProxy {
        fn drop(&mut self) {
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn forward_postgres_responses(
        provider: &mut TcpStream,
        client: &mut TcpStream,
        response_dropped: &AtomicBool,
    ) -> std::io::Result<()> {
        const MAX_BACKEND_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
        loop {
            let mut message_type = [0_u8; 1];
            match provider.read_exact(&mut message_type) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            }
            let mut encoded_length = [0_u8; 4];
            provider.read_exact(&mut encoded_length)?;
            let encoded_length = u32::from_be_bytes(encoded_length);
            let payload_length = encoded_length.checked_sub(4).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid PostgreSQL backend message length",
                )
            })?;
            let payload_length = usize::try_from(payload_length)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if payload_length > MAX_BACKEND_MESSAGE_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "PostgreSQL backend message exceeds proxy limit",
                ));
            }
            let mut payload = vec![0_u8; payload_length];
            provider.read_exact(&mut payload)?;
            if message_type == *b"C" && payload == b"COMMIT\0" {
                response_dropped.store(true, Ordering::Release);
                client.shutdown(Shutdown::Both)?;
                return Ok(());
            }
            client.write_all(&message_type)?;
            client.write_all(&encoded_length.to_be_bytes())?;
            client.write_all(&payload)?;
        }
    }

    // ------------------------------------------------------------------
    // PostgresReconstructionTermRecord: private type, tested in-module
    // ------------------------------------------------------------------
    #[test]
    fn reconstruction_term_record_roundtrips() {
        let hash = ShardlineHash::from_bytes([42; 32]);
        let range = ChunkRange::new(2, 5).unwrap();
        let term = ReconstructionTerm::new(StoredObjectId::new(hash), range, 512);

        let record = PostgresReconstructionTermRecord::from_domain(&term);
        assert_eq!(record.object_hash.len(), 64);
        assert_eq!(record.chunk_start, 2);
        assert_eq!(record.chunk_end_exclusive, 5);
        assert_eq!(record.unpacked_length, 512);

        let restored = record.into_domain().expect("valid reconstruction term");
        assert_eq!(restored, term);
    }

    #[test]
    fn reconstruction_term_record_invalid_hash_returns_error() {
        let record = PostgresReconstructionTermRecord {
            object_hash: "not-a-valid-hex-string".into(),
            chunk_start: 0,
            chunk_end_exclusive: 1,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        // "not-a-valid-hex-string" has length 22 (< 64), so it fails with InvalidLength
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(
                HashParseError::InvalidLength
            ))
        ));
    }

    #[test]
    fn reconstruction_term_record_invalid_hash_char_returns_error() {
        // 64 characters, but contains uppercase
        let hex_hash = "A".repeat(64);
        let record = PostgresReconstructionTermRecord {
            object_hash: hex_hash,
            chunk_start: 0,
            chunk_end_exclusive: 1,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(
                HashParseError::InvalidCharacter(_)
            ))
        ));
    }

    #[test]
    fn reconstruction_term_record_invalid_range_returns_error() {
        let hex_hash = "a".repeat(64);
        let record = PostgresReconstructionTermRecord {
            object_hash: hex_hash,
            chunk_start: 5,
            chunk_end_exclusive: 3,
            unpacked_length: 100,
        };
        let result = record.into_domain();
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::Range(_))
        ));
    }

    // ------------------------------------------------------------------
    // PostgresFileReconstructionRecord: pub(super) type
    // ------------------------------------------------------------------
    #[test]
    fn file_reconstruction_record_multiple_terms_roundtrips() {
        let hash_a = ShardlineHash::from_bytes([1; 32]);
        let hash_b = ShardlineHash::from_bytes([2; 32]);
        let range_a = ChunkRange::new(0, 1).unwrap();
        let range_b = ChunkRange::new(1, 3).unwrap();
        let reconstruction = FileReconstruction::new(vec![
            ReconstructionTerm::new(StoredObjectId::new(hash_a), range_a, 64),
            ReconstructionTerm::new(StoredObjectId::new(hash_b), range_b, 128),
        ]);
        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain().expect("valid reconstruction");
        assert_eq!(restored.terms().len(), 2);
    }

    #[test]
    fn file_reconstruction_record_empty_terms() {
        let reconstruction = FileReconstruction::new(vec![]);
        let record = PostgresFileReconstructionRecord::from_domain(&reconstruction);
        let restored = record.into_domain().expect("empty terms is valid");
        assert!(restored.terms().is_empty());
    }

    #[test]
    fn file_reconstruction_record_invalid_hash_in_terms_returns_error() {
        let record = PostgresFileReconstructionRecord {
            terms: vec![PostgresReconstructionTermRecord {
                object_hash: "bad".into(),
                chunk_start: 0,
                chunk_end_exclusive: 1,
                unpacked_length: 0,
            }],
        };
        let result = record.into_domain();
        assert!(matches!(
            result,
            Err(super::PostgresMetadataStoreError::HashParse(_))
        ));
    }

    // ── Postgres UploadIntentStore integration tests ──────────────────────

    use crate::upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore};
    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn postgres_upstream(database_url: &str) -> Option<String> {
        let parsed = url::Url::parse(database_url).ok()?;
        let host = parsed.host_str()?;
        let port = parsed.port().unwrap_or(5432);
        Some(format!("{host}:{port}"))
    }

    fn make_pg_store(pool: sqlx::PgPool) -> crate::PostgresIndexStore {
        crate::PostgresIndexStore::new(pool)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_repository_state_concurrent_partial_updates_are_merged() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = $1 AND owner = $2 AND repo = $3",
        )
        .bind(RepositoryProvider::GitHub.as_str())
        .bind("concurrent-team")
        .bind("concurrent-state")
        .execute(&pool)
        .await
        .expect("clean provider state fixture");

        let access_store = make_pg_store(pool.clone());
        let revision_store = make_pg_store(pool);
        let access = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "concurrent-team".into(),
            "concurrent-state".into(),
            Some(150),
            None,
            None,
        )
        .with_reconciliation(Some(170), None, Some(190));
        let revision = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "concurrent-team".into(),
            "concurrent-state".into(),
            None,
            Some(200),
            Some("refs/heads/main".into()),
        )
        .with_reconciliation(None, Some(180), None);

        let (access_result, revision_result) = tokio::join!(
            access_store.upsert_provider_repository_state(&access),
            revision_store.upsert_provider_repository_state(&revision),
        );
        access_result.expect("access state upsert");
        revision_result.expect("revision state upsert");

        let loaded = access_store
            .provider_repository_state(
                RepositoryProvider::GitHub,
                "concurrent-team",
                "concurrent-state",
            )
            .await
            .expect("load merged provider state")
            .expect("merged provider state");
        assert_eq!(loaded.last_access_changed_at_unix_seconds(), Some(150));
        assert_eq!(loaded.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(loaded.last_pushed_revision(), Some("refs/heads/main"));
        assert_eq!(loaded.last_cache_invalidated_at_unix_seconds(), Some(170));
        assert_eq!(
            loaded.last_authorization_rechecked_at_unix_seconds(),
            Some(180)
        );
        assert_eq!(loaded.last_drift_checked_at_unix_seconds(), Some(190));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_repository_state_tampered_evidence_is_rejected_on_read() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool.clone());
        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = 'evidence-team' AND repo = 'tampered'",
        )
        .execute(&pool)
        .await
        .expect("clean provider state fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent'
               AND operation_id = 'github:evidence-team:tampered'",
        )
        .execute(&pool)
        .await
        .expect("clean provider evidence fixture");
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "evidence-team".into(),
            "tampered".into(),
            Some(100),
            None,
            None,
        );
        store
            .upsert_provider_repository_state(&state)
            .await
            .expect("create provider evidence fixture");
        sqlx::query(
            "UPDATE shardline_reliability_events
             SET event_json = '{}'::jsonb
             WHERE operation_kind = 'ProviderEvent'
               AND operation_id = $1",
        )
        .bind(format!(
            "{}:evidence-team:tampered",
            RepositoryProvider::GitHub.as_str()
        ))
        .execute(&pool)
        .await
        .expect("tamper provider evidence fixture");
        assert!(
            store
                .provider_repository_state(RepositoryProvider::GitHub, "evidence-team", "tampered")
                .await
                .is_err()
        );
        assert!(
            store
                .delete_provider_repository_state(
                    RepositoryProvider::GitHub,
                    "evidence-team",
                    "tampered",
                )
                .await
                .is_err()
        );
        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = 'evidence-team' AND repo = 'tampered'",
        )
        .execute(&pool)
        .await
        .expect("clean provider state fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent'
               AND operation_id = 'github:evidence-team:tampered'",
        )
        .execute(&pool)
        .await
        .expect("clean provider evidence fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_provider_repository_state_rejects_missing_evidence_on_read() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool.clone());
        let owner = "evidence-repair-team";
        let repo = "missing-journal";
        let operation_id = format!("{}:{owner}:{repo}", RepositoryProvider::GitHub.as_str());
        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean provider state fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(&operation_id)
        .execute(&pool)
        .await
        .expect("clean provider evidence fixture");
        sqlx::query(
            "INSERT INTO shardline_provider_repository_states
                (provider, owner, repo, last_access_changed_at_unix_seconds)
             VALUES ('github', $1, $2, 100)",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("seed provider state without evidence");

        assert!(
            store
                .provider_repository_state(RepositoryProvider::GitHub, owner, repo)
                .await
                .is_err()
        );
        let event_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(&operation_id)
        .fetch_one(&pool)
        .await
        .expect("count evidence");
        assert_eq!(event_count, 0);

        sqlx::query(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = 'github' AND owner = $1 AND repo = $2",
        )
        .bind(owner)
        .bind(repo)
        .execute(&pool)
        .await
        .expect("clean state fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1",
        )
        .bind(&operation_id)
        .execute(&pool)
        .await
        .expect("clean evidence fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_quarantine_visitor_rejects_tampered_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool.clone());
        let object_key = shardline_storage::ObjectKey::parse("gc/visitor-tampered").unwrap();
        sqlx::query("DELETE FROM shardline_quarantine_candidates WHERE object_key = $1")
            .bind(object_key.as_str())
            .execute(&pool)
            .await
            .expect("clean quarantine fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("clean quarantine evidence fixture");

        let candidate = QuarantineCandidate::new(object_key.clone(), 4, 100, 200).unwrap();
        store
            .upsert_quarantine_candidate(&candidate)
            .await
            .expect("create quarantine fixture");
        sqlx::query(
            "UPDATE shardline_reliability_events
             SET event_json = '{}'::jsonb
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("tamper quarantine evidence fixture");

        let mut visited = false;
        let result = AsyncIndexStore::visit_quarantine_candidates(&store, |visited_candidate| {
            visited = true;
            assert_eq!(visited_candidate.object_key(), &object_key);
            Ok::<(), super::PostgresMetadataStoreError>(())
        })
        .await;
        assert!(result.is_err());
        assert!(!visited, "tampered state must not reach GC visitors");

        sqlx::query("DELETE FROM shardline_quarantine_candidates WHERE object_key = $1")
            .bind(object_key.as_str())
            .execute(&pool)
            .await
            .expect("clean quarantine fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("clean quarantine evidence fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_quarantine_delete_repairs_missing_baseline_chain() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool.clone());
        let object_key = shardline_storage::ObjectKey::parse("gc/delete-repair").unwrap();
        sqlx::query("DELETE FROM shardline_quarantine_candidates WHERE object_key = $1")
            .bind(object_key.as_str())
            .execute(&pool)
            .await
            .expect("clean quarantine fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("clean quarantine evidence fixture");
        let candidate = QuarantineCandidate::new(object_key.clone(), 4, 100, 200).unwrap();
        store
            .upsert_quarantine_candidate(&candidate)
            .await
            .expect("create quarantine fixture");
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("remove quarantine evidence fixture");

        assert!(
            store
                .delete_quarantine_candidate(&object_key)
                .await
                .expect("delete quarantine candidate")
        );
        let (count, minimum, maximum): (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), MIN(sequence), MAX(sequence)
             FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .fetch_one(&pool)
        .await
        .expect("inspect repaired quarantine evidence");
        assert_eq!((count, minimum, maximum), (2, 0, 1));

        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1",
        )
        .bind(object_key.as_str())
        .execute(&pool)
        .await
        .expect("clean repaired quarantine evidence");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_create_and_retrieve() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // A prior (possibly interrupted) run may have left this fixed id behind
        // in another state; purge it so the assertions observe a fresh row.
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind("test-intent-1")
            .execute(&pool)
            .await
            .expect("clean fixture");
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-1".into(),
            "test/key".into(),
            "ab".repeat(32),
            128,
        );
        store.create_intent(&intent).await.expect("create_intent");
        let loaded = store
            .intent_by_id("test-intent-1")
            .await
            .expect("intent_by_id");
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.intent_id(), "test-intent-1");
        assert_eq!(loaded.state(), UploadIntentState::Created);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_read_rejects_missing_evidence_without_writing() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let intent = UploadIntent::new(
            format!("repair-upload-evidence-{}", std::process::id()),
            "objects/repair-upload-evidence".into(),
            "e".repeat(64),
            42,
        );
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1",
        )
        .bind(intent.intent_id())
        .execute(&pool)
        .await
        .unwrap();
        let store = make_pg_store(pool.clone());
        store.create_intent(&intent).await.unwrap();
        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Stored)
                .await
                .unwrap()
        );
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1",
        )
        .bind(intent.intent_id())
        .execute(&pool)
        .await
        .unwrap();

        assert!(store.intent_by_id(intent.intent_id()).await.is_err());
        assert!(store.reliability_events(intent.intent_id()).await.is_err());
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1",
        )
        .bind(intent.intent_id())
        .execute(&pool)
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_create_idempotent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-idempotent".into(),
            "test/key2".into(),
            "cd".repeat(32),
            64,
        );
        store.create_intent(&intent).await.expect("first create");
        // Second create with same ID should not error (ON CONFLICT DO NOTHING)
        store
            .create_intent(&intent)
            .await
            .expect("second create (idempotent)");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_recovers_when_commit_response_is_lost() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let Some(direct_pool) = connect_postgres().await else {
            eprintln!("skipping: cannot connect to DATABASE_URL");
            return;
        };
        let intent = UploadIntent::new(
            "test-intent-lost-commit-response".into(),
            "test/lost-commit-response".into(),
            "9a".repeat(32),
            4096,
        );
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&direct_pool)
            .await
            .expect("clean commit-response-loss fixture");

        let upstream = postgres_upstream(&database_url).expect("Postgres upstream address");
        let proxy = CommitResponseLossProxy::start(upstream).expect("start Postgres fault proxy");
        let connect_options = PgConnectOptions::from_str(&database_url)
            .expect("parse DATABASE_URL")
            .host("127.0.0.1")
            .port(proxy.port())
            .ssl_mode(PgSslMode::Disable);
        let proxy_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("connect through Postgres fault proxy");
        let mut transaction = proxy_pool.begin().await.expect("begin transaction");
        sqlx::query(
            "INSERT INTO shardline_upload_intents (
                intent_id, object_key, object_hash, object_length, state, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, now(), now())",
        )
        .bind(intent.intent_id())
        .bind(intent.object_key())
        .bind(intent.object_hash())
        .bind(intent.object_length() as i64)
        .bind(intent.state().as_str())
        .execute(&mut *transaction)
        .await
        .expect("insert upload intent before ambiguous COMMIT");

        let commit = transaction.commit().await;
        assert!(
            proxy.response_was_dropped(),
            "proxy must observe PostgreSQL commit before dropping its completion message"
        );
        assert!(
            commit.is_err(),
            "client must see an ambiguous outcome when COMMIT completion is lost"
        );
        proxy_pool.close().await;

        let store = make_pg_store(direct_pool.clone());
        store
            .create_intent(&intent)
            .await
            .expect("idempotent retry after ambiguous COMMIT");
        let loaded = store
            .intent_by_id(intent.intent_id())
            .await
            .expect("read durable intent after ambiguous COMMIT")
            .expect("committed intent exists");
        assert_eq!(loaded.object_key(), intent.object_key());
        assert_eq!(loaded.object_hash(), intent.object_hash());
        assert_eq!(loaded.object_length(), intent.object_length());
        assert_eq!(loaded.state(), UploadIntentState::Created);

        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&direct_pool)
            .await
            .expect("clean commit-response-loss fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_pool_exhaustion_is_bounded_and_recovers() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // Establish one connection with a startup-sized timeout first.  The
        // assertion below is about bounded acquisition after the pool is
        // ready; a busy CI Postgres service must not make pool bootstrap
        // itself look like an exhaustion failure.
        let bootstrap_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&database_url)
            .await
            .expect("bootstrap Postgres pool");
        bootstrap_pool.close().await;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(100))
            .connect(&database_url)
            .await
            .expect("connect bounded Postgres pool");
        let held_connection = pool.acquire().await.expect("hold only pool connection");
        let exhausted = pool.acquire().await;
        assert!(
            matches!(exhausted, Err(sqlx::Error::PoolTimedOut)),
            "exhausted pool must fail within its configured bound: {exhausted:?}"
        );
        drop(held_connection);
        let recovered = pool.acquire().await;
        assert!(
            recovered.is_ok(),
            "pool must recover after capacity is released: {recovered:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_statement_timeout_is_retryable_after_lock_release() {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '100ms'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect statement-timeout Postgres pool");
        let store = make_pg_store(pool.clone());
        let intent = UploadIntent::new(
            "test-intent-statement-timeout".into(),
            "test/statement-timeout".into(),
            "7b".repeat(32),
            2048,
        );
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .expect("clean statement-timeout fixture");
        store
            .create_intent(&intent)
            .await
            .expect("create statement-timeout fixture");

        let mut lock_transaction = pool.begin().await.expect("begin row-lock transaction");
        sqlx::query(
            "SELECT intent_id FROM shardline_upload_intents WHERE intent_id = $1 FOR UPDATE",
        )
        .bind(intent.intent_id())
        .fetch_one(&mut *lock_transaction)
        .await
        .expect("lock upload-intent row");
        let timed_out = store
            .transition_intent(intent.intent_id(), UploadIntentState::Storing)
            .await;
        assert!(
            matches!(timed_out, Err(super::PostgresMetadataStoreError::Sqlx(_))),
            "blocked transition must surface the statement timeout: {timed_out:?}"
        );
        lock_transaction
            .rollback()
            .await
            .expect("release upload-intent row lock");

        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .expect("retry transition after lock release"),
            "retry must advance the intent after the transient lock clears"
        );
        let loaded = store
            .intent_by_id(intent.intent_id())
            .await
            .expect("load transitioned upload intent")
            .expect("transitioned upload intent exists");
        assert_eq!(loaded.state(), UploadIntentState::Storing);

        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent.intent_id())
            .execute(&pool)
            .await
            .expect("clean statement-timeout fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_rejects_id_reuse_for_different_object() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind("test-intent-conflict")
            .execute(&pool)
            .await
            .expect("clean leftover intent");
        let store = make_pg_store(pool);
        let original = UploadIntent::new(
            "test-intent-conflict".into(),
            "test/original".into(),
            "ab".repeat(32),
            64,
        );
        let conflicting = UploadIntent::new(
            "test-intent-conflict".into(),
            "test/conflicting".into(),
            "cd".repeat(32),
            128,
        );
        store.create_intent(&original).await.unwrap();
        store.create_intent(&original).await.unwrap();
        assert!(matches!(
            store.create_intent(&conflicting).await,
            Err(super::PostgresMetadataStoreError::UploadIntentConflict(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_transition_to_visible() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // Robust against a persistent Postgres: a previous run may have left
        // this fixed-id intent in a terminal state, which would make the forward
        // transition chain below fail. Clean the row so the test starts fresh
        // (CI's ephemeral database does not surface this).
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind("test-intent-transition")
            .execute(&pool)
            .await
            .expect("clean leftover intent");
        let store = make_pg_store(pool);
        let intent = UploadIntent::new(
            "test-intent-transition".into(),
            "test/key3".into(),
            "ef".repeat(32),
            256,
        );
        store.create_intent(&intent).await.expect("create_intent");
        for state in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
            UploadIntentState::Visible,
        ] {
            let transitioned = store
                .transition_intent("test-intent-transition", state)
                .await
                .expect("transition_intent");
            assert!(transitioned, "transition to {state:?} should succeed");
        }
        let loaded = store
            .intent_by_id("test-intent-transition")
            .await
            .expect("intent_by_id");
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().state(), UploadIntentState::Visible);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_scoped_intent_legacy_transition_preserves_evidence_identity() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let intent_id = "pg-scoped-legacy-transition";
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent_id)
            .execute(&pool)
            .await
            .expect("clean leftover intent");
        let store = make_pg_store(pool.clone());
        let intent = UploadIntent::new(
            intent_id.into(),
            "objects/pg-scoped".into(),
            "ab".repeat(32),
            7,
        );
        store
            .create_intent_scoped(&intent, "tenant-pg", "repo-pg")
            .await
            .expect("create scoped intent");
        assert!(
            store
                .transition_intent(intent_id, UploadIntentState::Storing)
                .await
                .expect("transition scoped intent")
        );
        let rows = sqlx::query(
            "SELECT event_json FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1 ORDER BY sequence",
        )
        .bind(intent_id)
        .fetch_all(&pool)
        .await
        .expect("load scoped events");
        let events = rows
            .into_iter()
            .map(|row| serde_json::from_value(row.try_get("event_json").unwrap()).unwrap())
            .collect::<Vec<shardline_reliability::LifecycleEvent>>();
        assert!(events.iter().all(|event| {
            event.operation.tenant == "tenant-pg" && event.operation.repository == "repo-pg"
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_scoped_intent_migrates_valid_legacy_evidence_identity() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let intent_id = "pg-scoped-legacy-identity-migration";
        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1",
        )
        .bind(intent_id)
        .execute(&pool)
        .await
        .expect("clean legacy evidence fixture");
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent_id)
            .execute(&pool)
            .await
            .expect("clean legacy intent fixture");

        let intent = UploadIntent::new(
            intent_id.into(),
            "objects/pg-legacy-identity".into(),
            "cd".repeat(32),
            7,
        );
        sqlx::query(
            "INSERT INTO shardline_upload_intents
                (intent_id, object_key, object_hash, object_length, state, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, now(), now())",
        )
        .bind(intent.intent_id())
        .bind(intent.object_key())
        .bind(intent.object_hash())
        .bind(intent.object_length() as i64)
        .bind(UploadIntentState::Visible.as_str())
        .execute(&pool)
        .await
        .expect("insert legacy intent fixture");

        let legacy_events = baseline_upload_lifecycle_events(
            "shardline",
            "default",
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            UploadIntentState::Visible,
        )
        .expect("build legacy evidence fixture");
        for event in &legacy_events {
            sqlx::query(
                "INSERT INTO shardline_reliability_events
                    (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
                 VALUES ($1, $2, $3, $4, 0)",
            )
            .bind(event.operation.kind.as_str())
            .bind(event.operation.operation_id.as_str())
            .bind(event.sequence as i64)
            .bind(serde_json::to_value(event).expect("serialize legacy event"))
            .execute(&pool)
            .await
            .expect("insert legacy evidence fixture");
        }

        let store = make_pg_store(pool.clone());
        store
            .create_intent_scoped(&intent, "tenant-pg", "repo-pg")
            .await
            .expect("migrate valid legacy identity");
        let rows = sqlx::query(
            "SELECT event_json FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1 ORDER BY sequence",
        )
        .bind(intent_id)
        .fetch_all(&pool)
        .await
        .expect("load migrated evidence");
        let events = rows
            .into_iter()
            .map(|row| serde_json::from_value(row.try_get("event_json").unwrap()).unwrap())
            .collect::<Vec<shardline_reliability::LifecycleEvent>>();
        assert_eq!(events.len(), legacy_events.len());
        assert!(events.iter().all(|event| {
            event.operation.tenant == "tenant-pg" && event.operation.repository == "repo-pg"
        }));

        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1",
        )
        .bind(intent_id)
        .execute(&pool)
        .await
        .expect("clean migrated evidence fixture");
        sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
            .bind(intent_id)
            .execute(&pool)
            .await
            .expect("clean migrated intent fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_transition_missing_returns_false() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = make_pg_store(pool);
        let result = store
            .transition_intent("nonexistent-intent", UploadIntentState::Failed)
            .await
            .expect("transition_intent");
        assert!(!result, "transitioning missing intent should return false");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_upload_intent_query_by_state() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        // Purge the fixed fixture ids so assertions observe only this run's rows.
        for id in ["query-state-a", "query-state-b"] {
            sqlx::query("DELETE FROM shardline_upload_intents WHERE intent_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("clean fixture");
        }
        let store = make_pg_store(pool);
        // Create two intents with different states
        let a = UploadIntent::new("query-state-a".into(), "test/a".into(), "01".repeat(32), 10);
        let b = UploadIntent::new("query-state-b".into(), "test/b".into(), "02".repeat(32), 20);
        store.create_intent(&a).await.expect("create a");
        store.create_intent(&b).await.expect("create b");
        store
            .transition_intent("query-state-b", UploadIntentState::Failed)
            .await
            .expect("transition b");

        let created = store
            .intents_by_state(UploadIntentState::Created)
            .await
            .expect("query created");
        let failed = store
            .intents_by_state(UploadIntentState::Failed)
            .await
            .expect("query failed");
        assert!(
            created.iter().any(|i| i.intent_id() == "query-state-a"),
            "a should be in Created"
        );
        assert!(
            failed.iter().any(|i| i.intent_id() == "query-state-b"),
            "b should be in Failed"
        );
    }
}
