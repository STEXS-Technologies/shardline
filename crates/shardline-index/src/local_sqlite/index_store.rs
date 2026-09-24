use rusqlite::{OptionalExtension, Transaction, params};
use shardline_protocol::{RepositoryProvider, ShardlineHash, unix_now_seconds_lossy};
use shardline_reliability::{
    LifecycleEvent, ProviderEvidenceLog, QuarantineLifecycleState, RetentionHoldLifecycleState,
    SnapshotEvidence, WebhookDeliveryLifecycleState, append_or_baseline_snapshot_evidence,
    upload_lifecycle_event, upload_lifecycle_identity, verify_and_append_snapshot_transition,
    verify_provider_lifecycle_events, verify_snapshot_evidence, verify_upload_lifecycle_events,
};
use shardline_storage::ObjectKey;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, u64_to_i64};
use crate::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LifecycleStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionStore, RetentionHold,
    StoredObjectId, WebhookDelivery,
    local_sqlite::helpers::{
        load_quarantine_evidence_batch, load_retention_evidence, load_retention_evidence_batch,
        load_webhook_evidence, load_webhook_evidence_batch, persist_retention_evidence,
        persist_webhook_evidence, retention_snapshot, webhook_snapshot,
    },
    parse_xet_hash_hex,
    provider_evidence::snapshot_from_state,
    upload_intent::{UploadIntent, UploadIntentState, UploadIntentStore},
    xet_hash_hex_string,
};

fn verify_sqlite_intent_evidence(
    transaction: &Transaction<'_>,
    intent: &UploadIntent,
) -> Result<(), LocalIndexStoreError> {
    let mut statement = transaction.prepare(
        "SELECT event_json
         FROM shardline_reliability_events
         WHERE operation_kind = ?1 AND operation_id = ?2
         ORDER BY sequence",
    )?;
    let rows = statement.query_map(params!["Upload", intent.intent_id()], |row| {
        let event_json: String = row.get(0)?;
        serde_json::from_str(&event_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })?;
    let events = rows.collect::<Result<Vec<LifecycleEvent>, _>>()?;
    let (tenant, repository) = upload_lifecycle_identity(&events);
    verify_upload_lifecycle_events(
        &events,
        tenant,
        repository,
        intent.intent_id(),
        intent.object_key(),
        intent.object_hash(),
        intent.state(),
    )
    .map_err(LocalIndexStoreError::Reliability)
}

impl ReconstructionStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT terms
                 FROM shardline_file_reconstructions
                 WHERE file_id = ?1",
                params![xet_hash_hex_string(file_id.hash())],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| super::helpers::parse_reconstruction_json(&value))
            .transpose()
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT file_id
             FROM shardline_file_reconstructions
             ORDER BY file_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut file_ids = Vec::new();
        for row in rows {
            let hash = parse_xet_hash_hex(&row?)?;
            file_ids.push(FileId::new(hash));
        }
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_file_reconstructions WHERE file_id = ?1",
            params![xet_hash_hex_string(file_id.hash())],
        )?;
        Ok(changed > 0)
    }

    fn delete_reconstruction_if_matches(
        &self,
        file_id: &FileId,
        expected: &FileReconstruction,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let Some(terms) = transaction
            .query_row(
                "SELECT terms FROM shardline_file_reconstructions WHERE file_id = ?1",
                params![xet_hash_hex_string(file_id.hash())],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            transaction.commit()?;
            return Ok(false);
        };
        if super::helpers::parse_reconstruction_json(&terms)? != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_file_reconstructions WHERE file_id = ?1 AND terms = ?2",
            params![xet_hash_hex_string(file_id.hash()), terms],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn contains_object(&self, object_id: &StoredObjectId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let exists = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM shardline_stored_objects WHERE object_hash = ?1
            )",
            params![xet_hash_hex_string(object_id.hash())],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }
}

impl DedupeStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        let result: Result<Option<DedupeShardMapping>, _> = connection
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = ?1",
                params![xet_hash_hex_string(chunk_hash)],
                super::helpers::dedupe_shard_mapping_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from);
        let hit = result.as_ref().is_ok_and(|r| r.is_some());
        shardline_metrics::record_xet_dedupe_shard_query(hit);
        result
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT chunk_hash, shard_object_key
             FROM shardline_dedupe_shards
             ORDER BY chunk_hash",
        )?;
        let rows = statement.query_map([], super::helpers::dedupe_shard_mapping_from_row)?;
        collect_rows(rows)
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        for mapping in DedupeStore::list_dedupe_shard_mappings(self).map_err(Into::into)? {
            visitor(mapping)?;
        }
        Ok(())
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_dedupe_shards WHERE chunk_hash = ?1",
            params![xet_hash_hex_string(chunk_hash)],
        )?;
        Ok(changed > 0)
    }

    fn delete_dedupe_shard_mapping_if_matches(
        &self,
        expected: &DedupeShardMapping,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let Some(current) = transaction
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards WHERE chunk_hash = ?1",
                params![xet_hash_hex_string(expected.chunk_hash())],
                super::helpers::dedupe_shard_mapping_from_row,
            )
            .optional()?
        else {
            transaction.commit()?;
            return Ok(false);
        };
        if current != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_dedupe_shards
             WHERE chunk_hash = ?1 AND shard_object_key = ?2",
            params![
                xet_hash_hex_string(expected.chunk_hash()),
                expected.shard_object_key().as_str(),
            ],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }
}

impl LifecycleStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let candidate = transaction
            .query_row(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        if let Some(candidate) = &candidate {
            let snapshot =
                super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
            let evidence =
                super::helpers::load_quarantine_evidence(&transaction, object_key.as_str())?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(candidate)
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
             FROM shardline_quarantine_candidates
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::quarantine_candidate_from_row)?;
        let candidates = collect_rows(rows)?;
        drop(statement);
        let object_keys = candidates
            .iter()
            .map(|candidate| candidate.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_quarantine_evidence_batch(&transaction, &object_keys)?;
        for candidate in &candidates {
            let snapshot =
                super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
            let evidence = evidence.get(candidate.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(candidates)
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        for candidate in LifecycleStore::list_quarantine_candidates(self).map_err(Into::into)? {
            visitor(candidate)?;
        }
        Ok(())
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let previous = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![candidate.object_key().as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        transaction.execute(
            "INSERT INTO shardline_quarantine_candidates (
                object_key,
                observed_length,
                first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                observed_length = excluded.observed_length,
                first_seen_unreachable_at_unix_seconds =
                    excluded.first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds = excluded.delete_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                candidate.object_key().as_str(),
                u64_to_i64(candidate.observed_length())?,
                u64_to_i64(candidate.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(candidate.delete_after_unix_seconds())?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        let snapshot =
            super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Active)?;
        let evidence = super::helpers::load_quarantine_evidence(
            &transaction,
            candidate.object_key().as_str(),
        )?;
        let (evidence, evidence_was_empty) = if let Some(previous) = previous {
            let before =
                super::helpers::quarantine_snapshot(&previous, QuarantineLifecycleState::Active)?;
            verify_and_append_snapshot_transition(evidence, before, snapshot)?
        } else if evidence.events().is_empty() {
            (
                append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                true,
            )
        } else {
            let released =
                super::helpers::quarantine_snapshot(candidate, QuarantineLifecycleState::Released)?;
            verify_and_append_snapshot_transition(evidence, released, snapshot)?
        };
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "quarantine evidence event",
            ))
        })?;
        if evidence_was_empty {
            for stored_event in evidence.events() {
                super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
            }
        } else {
            super::helpers::persist_quarantine_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let candidate = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        let changed = transaction.execute(
            "DELETE FROM shardline_quarantine_candidates WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        if let Some(candidate) = candidate {
            let active =
                super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
            let released = super::helpers::quarantine_snapshot(
                &candidate,
                QuarantineLifecycleState::Released,
            )?;
            let evidence =
                super::helpers::load_quarantine_evidence(&transaction, object_key.as_str())?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            let event = evidence.events().last().ok_or_else(|| {
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "quarantine evidence event",
                    ),
                )
            })?;
            if evidence_was_empty {
                for stored_event in evidence.events() {
                    super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
                }
            } else {
                super::helpers::persist_quarantine_evidence(&transaction, event)?;
            }
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_quarantine_candidate_if_matches(
        &self,
        expected: &QuarantineCandidate,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let candidate = transaction
            .query_row(
                "SELECT object_key, observed_length, first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
                 FROM shardline_quarantine_candidates WHERE object_key = ?1",
                params![expected.object_key().as_str()],
                super::helpers::quarantine_candidate_from_row,
            )
            .optional()?;
        let Some(candidate) = candidate else {
            transaction.commit()?;
            return Ok(false);
        };
        if candidate != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active =
            super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
        let released =
            super::helpers::quarantine_snapshot(&candidate, QuarantineLifecycleState::Released)?;
        let evidence =
            super::helpers::load_quarantine_evidence(&transaction, expected.object_key().as_str())?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_quarantine_candidates
             WHERE object_key = ?1 AND observed_length = ?2
               AND first_seen_unreachable_at_unix_seconds = ?3
               AND delete_after_unix_seconds = ?4",
            params![
                expected.object_key().as_str(),
                u64_to_i64(expected.observed_length())?,
                u64_to_i64(expected.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(expected.delete_after_unix_seconds())?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "quarantine evidence event",
            ))
        })?;
        if evidence_was_empty {
            for stored_event in evidence.events() {
                super::helpers::persist_quarantine_evidence(&transaction, stored_event)?;
            }
        } else {
            super::helpers::persist_quarantine_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let hold = transaction
            .query_row(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        if let Some(hold) = hold.as_ref() {
            let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
            let evidence = load_retention_evidence(&transaction, object_key.as_str())?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(hold)
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT object_key,
                    reason,
                    held_at_unix_seconds,
                    release_after_unix_seconds
             FROM shardline_retention_holds
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::retention_hold_from_row)?;
        let holds = collect_rows(rows)?;
        drop(statement);
        let object_keys = holds
            .iter()
            .map(|hold| hold.object_key().as_str().to_owned())
            .collect::<Vec<_>>();
        let evidence = load_retention_evidence_batch(&transaction, &object_keys)?;
        for hold in &holds {
            let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
            let evidence = evidence.get(hold.object_key().as_str()).ok_or(
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ),
            )?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(holds)
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        for hold in LifecycleStore::list_retention_holds(self).map_err(Into::into)? {
            visitor(hold)?;
        }
        Ok(())
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let previous = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![hold.object_key().as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let snapshot = retention_snapshot(hold, RetentionHoldLifecycleState::Active)?;
        let evidence = load_retention_evidence(&transaction, hold.object_key().as_str())?;
        let (evidence, evidence_was_empty) = if let Some(previous) = previous.as_ref() {
            let previous_snapshot =
                retention_snapshot(previous, RetentionHoldLifecycleState::Active)?;
            verify_and_append_snapshot_transition(evidence, previous_snapshot, snapshot)?
        } else if evidence.events().is_empty() {
            (
                append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                true,
            )
        } else {
            let released = retention_snapshot(hold, RetentionHoldLifecycleState::Released)?;
            verify_and_append_snapshot_transition(evidence, released, snapshot)?
        };
        transaction.execute(
            "INSERT INTO shardline_retention_holds (
                object_key,
                reason,
                held_at_unix_seconds,
                release_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                reason = excluded.reason,
                held_at_unix_seconds = excluded.held_at_unix_seconds,
                release_after_unix_seconds = excluded.release_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                hold.object_key().as_str(),
                hold.reason(),
                u64_to_i64(hold.held_at_unix_seconds())?,
                hold.release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_retention_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_retention_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let hold = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![object_key.as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let changed = transaction.execute(
            "DELETE FROM shardline_retention_holds WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        if let Some(hold) = hold {
            let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
            let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
            let evidence = load_retention_evidence(&transaction, object_key.as_str())?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, active, released)?;
            if evidence_was_empty {
                for event in evidence.events() {
                    persist_retention_evidence(&transaction, event)?;
                }
            } else if let Some(event) = evidence.events().last() {
                persist_retention_evidence(&transaction, event)?;
            }
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_retention_hold_if_matches(
        &self,
        expected: &RetentionHold,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let hold = transaction
            .query_row(
                "SELECT object_key, reason, held_at_unix_seconds, release_after_unix_seconds
                 FROM shardline_retention_holds WHERE object_key = ?1",
                params![expected.object_key().as_str()],
                super::helpers::retention_hold_from_row,
            )
            .optional()?;
        let Some(hold) = hold else {
            transaction.commit()?;
            return Ok(false);
        };
        if hold != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active = retention_snapshot(&hold, RetentionHoldLifecycleState::Active)?;
        let released = retention_snapshot(&hold, RetentionHoldLifecycleState::Released)?;
        let evidence = load_retention_evidence(&transaction, expected.object_key().as_str())?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_retention_holds
             WHERE object_key = ?1 AND reason = ?2 AND held_at_unix_seconds = ?3
               AND (release_after_unix_seconds IS ?4)",
            params![
                expected.object_key().as_str(),
                expected.reason(),
                u64_to_i64(expected.held_at_unix_seconds())?,
                expected
                    .release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        if evidence_was_empty {
            for event in evidence.events() {
                persist_retention_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_retention_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    delivery.provider().as_str(),
                    delivery.owner(),
                    delivery.repo(),
                    delivery.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        if let Some(existing) = existing {
            let snapshot = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
            let evidence = load_webhook_evidence(&transaction, &existing)?;
            verify_snapshot_evidence(&evidence, &snapshot)?;
            transaction.commit()?;
            return Ok(false);
        }
        let snapshot = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
        let evidence = load_webhook_evidence(&transaction, delivery)?;
        let (evidence, evidence_was_empty) = if evidence.events().is_empty() {
            (
                append_or_baseline_snapshot_evidence(evidence, snapshot)?,
                true,
            )
        } else {
            let released = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Released)?;
            verify_and_append_snapshot_transition(evidence, released, snapshot)?
        };
        transaction.execute(
            "INSERT INTO shardline_webhook_deliveries (
                provider,
                owner,
                repo,
                delivery_id,
                processed_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (provider, owner, repo, delivery_id) DO NOTHING",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
                u64_to_i64(delivery.processed_at_unix_seconds())?,
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    delivery_id,
                    processed_at_unix_seconds
             FROM shardline_webhook_deliveries
             ORDER BY provider, owner, repo, delivery_id",
        )?;
        let rows = statement.query_map([], super::helpers::webhook_delivery_from_row)?;
        let deliveries = collect_rows(rows)?;
        drop(statement);
        let evidence = load_webhook_evidence_batch(&transaction, &deliveries)?;
        for delivery in &deliveries {
            let snapshot = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
            let operation_id = snapshot.evidence_operation()?.operation_id;
            let evidence = evidence
                .get(&operation_id)
                .ok_or(LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ))?;
            verify_snapshot_evidence(evidence, &snapshot)?;
        }
        transaction.commit()?;
        Ok(deliveries)
    }

    fn visit_webhook_deliveries<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
    {
        for delivery in LifecycleStore::list_webhook_deliveries(self).map_err(Into::into)? {
            visitor(delivery)?;
        }
        Ok(())
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    delivery.provider().as_str(),
                    delivery.owner(),
                    delivery.repo(),
                    delivery.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        let Some(existing) = existing else {
            transaction.commit()?;
            return Ok(false);
        };
        let snapshot = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
        let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
        let evidence = load_webhook_evidence(&transaction, &existing)?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, snapshot, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
            ],
        )?;
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(changed > 0)
    }

    fn delete_webhook_delivery_if_matches(
        &self,
        expected: &WebhookDelivery,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
                 FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    expected.provider().as_str(),
                    expected.owner(),
                    expected.repo(),
                    expected.delivery_id(),
                ],
                super::helpers::webhook_delivery_from_row,
            )
            .optional()?;
        let Some(existing) = existing else {
            transaction.commit()?;
            return Ok(false);
        };
        if existing != *expected {
            transaction.commit()?;
            return Ok(false);
        }
        let active = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Processed)?;
        let released = webhook_snapshot(&existing, WebhookDeliveryLifecycleState::Released)?;
        let evidence = load_webhook_evidence(&transaction, &existing)?;
        let (evidence, evidence_was_empty) =
            verify_and_append_snapshot_transition(evidence, active, released)?;
        let changed = transaction.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4
               AND processed_at_unix_seconds = ?5",
            params![
                expected.provider().as_str(),
                expected.owner(),
                expected.repo(),
                expected.delivery_id(),
                u64_to_i64(expected.processed_at_unix_seconds())?,
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        if evidence_was_empty {
            for event in evidence.events() {
                persist_webhook_evidence(&transaction, event)?;
            }
        } else if let Some(event) = evidence.events().last() {
            persist_webhook_evidence(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(true)
    }

    fn purge_webhook_deliveries_older_than(
        &self,
        older_than_unix_seconds: u64,
    ) -> Result<u64, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
             FROM shardline_webhook_deliveries
             WHERE processed_at_unix_seconds < ?1",
        )?;
        let rows = statement.query_map(
            params![u64_to_i64(older_than_unix_seconds)?],
            super::helpers::webhook_delivery_from_row,
        )?;
        let deliveries = collect_rows(rows)?;
        drop(statement);
        for delivery in &deliveries {
            let snapshot = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?;
            let released = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Released)?;
            let evidence = load_webhook_evidence(&transaction, delivery)?;
            let (evidence, evidence_was_empty) =
                verify_and_append_snapshot_transition(evidence, snapshot, released)?;
            transaction.execute(
                "DELETE FROM shardline_webhook_deliveries
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
                params![
                    delivery.provider().as_str(),
                    delivery.owner(),
                    delivery.repo(),
                    delivery.delivery_id(),
                ],
            )?;
            if evidence_was_empty {
                for event in evidence.events() {
                    persist_webhook_evidence(&transaction, event)?;
                }
            } else if let Some(event) = evidence.events().last() {
                persist_webhook_evidence(&transaction, event)?;
            }
        }
        transaction.commit()?;
        Ok(u64::try_from(deliveries.len()).unwrap_or(u64::MAX))
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let state = transaction
            .query_row(
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
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![provider.as_str(), owner, repo],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        if let Some(state) = state.as_ref() {
            let snapshot = snapshot_from_state(state)?;
            let stored = super::helpers::load_provider_evidence(&transaction, &snapshot)?;
            if stored.events().is_empty() {
                verify_provider_lifecycle_events(
                    ProviderEvidenceLog::baseline(snapshot.clone())?.events(),
                    &snapshot,
                )?;
            } else {
                stored.verify_for(&snapshot)?;
            }
        }
        transaction.commit()?;
        Ok(state)
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let mut connection = self.open_connection()?;
        let states = {
            let mut statement = connection.prepare(
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
            )?;
            let rows =
                statement.query_map([], super::helpers::provider_repository_state_from_row)?;
            collect_rows(rows)?
        };
        let transaction = connection.transaction()?;
        for state in &states {
            let snapshot = snapshot_from_state(state)?;
            let stored = super::helpers::load_provider_evidence(&transaction, &snapshot)?;
            if stored.events().is_empty() {
                verify_provider_lifecycle_events(
                    ProviderEvidenceLog::baseline(snapshot.clone())?.events(),
                    &snapshot,
                )?;
            } else {
                stored.verify_for(&snapshot)?;
            }
        }
        transaction.commit()?;
        Ok(states)
    }

    fn visit_provider_repository_states<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
    {
        for state in LifecycleStore::list_provider_repository_states(self).map_err(Into::into)? {
            visitor(state)?;
        }
        Ok(())
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
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
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![state.provider().as_str(), state.owner(), state.repo()],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        let current_snapshot = current.as_ref().map(snapshot_from_state).transpose()?;
        let evidence = if let Some(snapshot) = current_snapshot.as_ref() {
            super::helpers::load_provider_evidence(&transaction, snapshot)?
        } else {
            transaction.execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent' AND operation_id = ?1",
                params![format!(
                    "{}:{}:{}",
                    state.provider().as_str(),
                    state.owner(),
                    state.repo()
                )],
            )?;
            ProviderEvidenceLog::default()
        };
        let now = unix_now_seconds_lossy();
        transaction.execute(
            "INSERT INTO shardline_provider_repository_states (
                provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds,
                created_at_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (provider, owner, repo)
             DO UPDATE SET
                last_access_changed_at_unix_seconds = CASE
                    WHEN excluded.last_access_changed_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_access_changed_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_access_changed_at_unix_seconds IS NULL
                      OR excluded.last_access_changed_at_unix_seconds >= shardline_provider_repository_states.last_access_changed_at_unix_seconds
                        THEN excluded.last_access_changed_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_access_changed_at_unix_seconds
                END,
                last_pushed_revision = CASE
                    WHEN excluded.last_revision_pushed_at_unix_seconds IS NOT NULL
                     AND (shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                       OR excluded.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds)
                        THEN excluded.last_pushed_revision
                    ELSE shardline_provider_repository_states.last_pushed_revision
                END,
                last_revision_pushed_at_unix_seconds = CASE
                    WHEN excluded.last_revision_pushed_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_revision_pushed_at_unix_seconds IS NULL
                      OR excluded.last_revision_pushed_at_unix_seconds >= shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                        THEN excluded.last_revision_pushed_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_revision_pushed_at_unix_seconds
                END,
                last_cache_invalidated_at_unix_seconds = CASE
                    WHEN excluded.last_cache_invalidated_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds IS NULL
                      OR excluded.last_cache_invalidated_at_unix_seconds >= shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                        THEN excluded.last_cache_invalidated_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_cache_invalidated_at_unix_seconds
                END,
                last_authorization_rechecked_at_unix_seconds = CASE
                    WHEN excluded.last_authorization_rechecked_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds IS NULL
                      OR excluded.last_authorization_rechecked_at_unix_seconds >= shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                        THEN excluded.last_authorization_rechecked_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_authorization_rechecked_at_unix_seconds
                END,
                last_drift_checked_at_unix_seconds = CASE
                    WHEN excluded.last_drift_checked_at_unix_seconds IS NULL
                        THEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                    WHEN shardline_provider_repository_states.last_drift_checked_at_unix_seconds IS NULL
                      OR excluded.last_drift_checked_at_unix_seconds >= shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                        THEN excluded.last_drift_checked_at_unix_seconds
                    ELSE shardline_provider_repository_states.last_drift_checked_at_unix_seconds
                END,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                state.provider().as_str(),
                state.owner(),
                state.repo(),
                state
                    .last_access_changed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_revision_pushed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state.last_pushed_revision(),
                state
                    .last_cache_invalidated_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_authorization_rechecked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_drift_checked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(now)?,
                u64_to_i64(now)?,
            ],
        )?;
        let merged = transaction.query_row(
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
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![state.provider().as_str(), state.owner(), state.repo()],
            super::helpers::provider_repository_state_from_row,
        )?;
        let snapshot = snapshot_from_state(&merged)?;
        let evidence = if let Some(before) = current_snapshot {
            verify_and_append_snapshot_transition(evidence, before, snapshot)?.0
        } else {
            append_or_baseline_snapshot_evidence(evidence, snapshot)?
        };
        let event = evidence.events().last().ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "provider evidence",
            ))
        })?;
        super::helpers::persist_provider_evidence(&transaction, event)?;
        transaction.commit()?;
        Ok(())
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
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
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![provider.as_str(), owner, repo],
                super::helpers::provider_repository_state_from_row,
            )
            .optional()?;
        if let Some(state) = current {
            let snapshot = snapshot_from_state(&state)?;
            let evidence = super::helpers::load_provider_evidence(&transaction, &snapshot)?;
            if evidence.events().is_empty() {
                let baseline = ProviderEvidenceLog::baseline(snapshot.clone())?;
                verify_provider_lifecycle_events(baseline.events(), &snapshot)?;
                for event in baseline.events() {
                    super::helpers::persist_provider_evidence(&transaction, event)?;
                }
            } else {
                verify_provider_lifecycle_events(evidence.events(), &snapshot)?;
            }
        }
        let changed = transaction.execute(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![provider.as_str(), owner, repo],
        )?;
        transaction.execute(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = ?1",
            params![format!("{}:{}:{}", provider.as_str(), owner, repo)],
        )?;
        transaction.commit()?;
        Ok(changed > 0)
    }
}

#[async_trait::async_trait]
impl UploadIntentStore for super::LocalIndexStore {
    type Error = LocalIndexStoreError;

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
        let store = self.clone();
        let intent = intent.clone();
        let tenant = tenant.to_owned();
        let repository = repository.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            let created_event = upload_lifecycle_event(
                &tenant,
                &repository,
                intent.intent_id(),
                intent.object_key(),
                intent.object_hash(),
                shardline_reliability::UploadLifecycleState::Created,
                shardline_reliability::UploadLifecycleState::Created,
            )?;
            let transaction = conn.transaction()?;
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO shardline_upload_intents (intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    intent.intent_id(),
                    intent.object_key(),
                    intent.object_hash(),
                    intent.object_length() as i64,
                    intent.state().as_str(),
                    now,
                    now,
                ],
            )?;
            if inserted == 0 {
                let matches_identity = transaction.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM shardline_upload_intents
                        WHERE intent_id = ?1 AND object_key = ?2 AND object_hash = ?3
                          AND object_length = ?4
                     )",
                    rusqlite::params![
                        intent.intent_id(),
                        intent.object_key(),
                        intent.object_hash(),
                        intent.object_length() as i64,
                    ],
                    |row| row.get::<_, bool>(0),
                )?;
                if !matches_identity {
                    return Err(crate::UploadIntentConflictError::new(intent.intent_id()).into());
                }
            } else {
                transaction.execute(
                    "DELETE FROM shardline_reliability_events
                     WHERE operation_kind = 'Upload' AND operation_id = ?1",
                    rusqlite::params![intent.intent_id()],
                )?;
                super::helpers::persist_reliability_event_at(&transaction, &created_event, now)?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
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
            // Idempotent: the intent is already in the target state. This happens
            // when a duplicate concurrent caller performs the same transition, so
            // treat it as success rather than an invalid transition.
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
        let store = self.clone();
        let intent_id = intent_id.to_owned();
        let current_state = current.state();
        let event_json = serde_json::to_string(event)?;
        let event = event.clone();
        let operation_id = event.operation.operation_id.clone();
        let transitioned = tokio::task::spawn_blocking(move || -> Result<bool, LocalIndexStoreError> {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            let rows = transaction.execute(
                "UPDATE shardline_upload_intents SET state = ?1, updated_at_unix_seconds = ?2 WHERE intent_id = ?3 AND state = ?4",
                rusqlite::params![new_state.as_str(), now, intent_id, current_state.as_str()],
            )?;
            if rows == 0 {
                transaction.rollback()?;
                return Ok(false);
            }
            super::helpers::persist_reliability_event_at(&transaction, &event, now)?;
            let stored_event: String = transaction.query_row(
                "SELECT event_json FROM shardline_reliability_events WHERE operation_kind = ?1 AND operation_id = ?2 AND sequence = ?3",
                rusqlite::params![
                    event.operation.kind.as_str(),
                    &event.operation.operation_id,
                    u64_to_i64(event.sequence)?,
                ],
                |row| row.get(0),
            )?;
            if stored_event != event_json {
                transaction.rollback()?;
                return Err(LocalIndexStoreError::ReliabilityEventConflict(
                    operation_id,
                ));
            }
            transaction.commit()?;
            Ok(true)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))??;
        Ok(transitioned)
    }

    async fn intent_by_id(&self, intent_id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        let store = self.clone();
        let intent_id = intent_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE intent_id = ?1"
            )?;
            let result = stmt.query_row(rusqlite::params![intent_id], |row| {
                let state_str: String = row.get(4)?;
                let state = UploadIntentState::parse(&state_str).ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                })?;
                Ok(UploadIntent::from_parts(
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get::<_, i64>(3)? as u64,
                    state,
                    Duration::from_secs(row.get::<_, i64>(5)? as u64),
                    Duration::from_secs(row.get::<_, i64>(6)? as u64),
                ))
            });
            match result {
                Ok(intent) => {
                    drop(stmt);
                    verify_sqlite_intent_evidence(&transaction, &intent)?;
                    transaction.commit()?;
                    Ok(Some(intent))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    drop(stmt);
                    transaction.commit()?;
                    Ok(None)
                }
                Err(e) => Err(LocalIndexStoreError::from(e)),
            }
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE state = ?1 ORDER BY created_at_unix_seconds"
            )?;
            let intents = stmt
                .query_map(rusqlite::params![state.as_str()], |row| {
                    let state_str: String = row.get(4)?;
                    let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                        rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                    })?;
                    Ok(UploadIntent::from_parts(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        s,
                        Duration::from_secs(row.get::<_, i64>(5)? as u64),
                        Duration::from_secs(row.get::<_, i64>(6)? as u64),
                    ))
                })
                .map_err(LocalIndexStoreError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(LocalIndexStoreError::from)?;
            drop(stmt);
            for intent in &intents {
                verify_sqlite_intent_evidence(&transaction, intent)?;
            }
            transaction.commit()?;
            Ok(intents)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let cutoff = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .saturating_sub(older_than)
                .as_secs() as i64;
            let mut stmt = transaction.prepare(
                "SELECT intent_id, object_key, object_hash, object_length, state, created_at_unix_seconds, updated_at_unix_seconds FROM shardline_upload_intents WHERE state = ?1 AND created_at_unix_seconds < ?2 ORDER BY created_at_unix_seconds"
            )?;
            let intents = stmt
                .query_map(rusqlite::params![state.as_str(), cutoff], |row| {
                    let state_str: String = row.get(4)?;
                    let s = UploadIntentState::parse(&state_str).ok_or_else(|| {
                        rusqlite::Error::InvalidColumnType(4, format!("invalid state: {state_str}"), rusqlite::types::Type::Text)
                    })?;
                    Ok(UploadIntent::from_parts(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                        s,
                        Duration::from_secs(row.get::<_, i64>(5)? as u64),
                        Duration::from_secs(row.get::<_, i64>(6)? as u64),
                    ))
                })
                .map_err(LocalIndexStoreError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(LocalIndexStoreError::from)?;
            drop(stmt);
            for intent in &intents {
                verify_sqlite_intent_evidence(&transaction, intent)?;
            }
            transaction.commit()?;
            Ok(intents)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn record_reliability_event(&self, event: &LifecycleEvent) -> Result<(), Self::Error> {
        event.verify_integrity()?;
        let store = self.clone();
        let event = event.clone();
        let event_json = serde_json::to_string(&event)?;
        let operation_id = event.operation.operation_id.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let (object_key, object_hash, state_text) = transaction
                .query_row(
                    "SELECT object_key, object_hash, state
                     FROM shardline_upload_intents
                     WHERE intent_id = ?1",
                    params![&operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::EmptyField(
                            "reliability event has no authoritative upload intent",
                        ),
                    )
                })?;
            let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                LocalIndexStoreError::Reliability(
                    shardline_reliability::ReliabilityError::EmptyField(
                        "unknown upload intent state",
                    ),
                )
            })?;
            let mut events = super::helpers::load_verified_event_json(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_id,
            )?
            .into_iter()
            .map(serde_json::from_value::<LifecycleEvent>)
            .collect::<Result<Vec<_>, _>>()?;
            if let Some(existing) = events
                .iter()
                .find(|existing| existing.sequence == event.sequence)
            {
                if existing != &event {
                    return Err(LocalIndexStoreError::ReliabilityEventConflict(
                        operation_id,
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
                &operation_id,
                &object_key,
                &object_hash,
                state,
            )
            .map_err(LocalIndexStoreError::Reliability)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            super::helpers::persist_reliability_event_at(&transaction, &event, now)?;
            let stored_event: String = transaction.query_row(
                "SELECT event_json FROM shardline_reliability_events WHERE operation_kind = ?1 AND operation_id = ?2 AND sequence = ?3",
                rusqlite::params![
                    event.operation.kind.as_str(),
                    &event.operation.operation_id,
                    u64_to_i64(event.sequence)?,
                ],
                |row| row.get(0),
            )?;
            if stored_event != event_json {
                return Err(LocalIndexStoreError::ReliabilityEventConflict(
                    operation_id,
                ));
            }
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }

    async fn reliability_events(
        &self,
        operation_id: &str,
    ) -> Result<Vec<LifecycleEvent>, Self::Error> {
        let store = self.clone();
        let operation_id = operation_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut conn = store.open_connection()?;
            let transaction = conn.transaction()?;
            let events = super::helpers::load_verified_event_json(
                &transaction,
                shardline_reliability::OperationKind::Upload,
                &operation_id,
            )?
            .into_iter()
            .map(serde_json::from_value::<LifecycleEvent>)
            .collect::<Result<Vec<_>, _>>()?;
            let intent = transaction
                .query_row(
                    "SELECT object_key, object_hash, state
                     FROM shardline_upload_intents
                     WHERE intent_id = ?1",
                    rusqlite::params![&operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((object_key, object_hash, state_text)) = intent {
                let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
                    LocalIndexStoreError::Reliability(
                        shardline_reliability::ReliabilityError::EmptyField(
                            "unknown upload intent state",
                        ),
                    )
                })?;
                let (tenant, repository) = upload_lifecycle_identity(&events);
                shardline_reliability::verify_upload_lifecycle_events(
                    &events,
                    tenant,
                    repository,
                    &operation_id,
                    &object_key,
                    &object_hash,
                    state,
                )?;
            } else {
                shardline_reliability::verify_lifecycle_chain(&events)
                    .map_err(LocalIndexStoreError::Reliability)?;
            }
            transaction.commit()?;
            Ok(events)
        })
        .await
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?
    }
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
    use shardline_protocol::{ChunkRange, RepositoryProvider};
    use shardline_reliability::SnapshotEvidence;
    use shardline_storage::ObjectKey;

    use super::*;
    use crate::{
        ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RetentionHold,
        WebhookDelivery,
    };

    fn make_store() -> LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalIndexStore::new(storage.path_buf()).expect("failed to create local index store")
    }

    #[test]
    fn insert_and_get_reconstruction_roundtrip() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([1; 32]));
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([2; 32]));
        let range = ChunkRange::new(0, 3).unwrap();
        let reconstruction =
            FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 256)]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let loaded =
            ReconstructionStore::reconstruction(&store, &file_id).expect("lookup should succeed");
        assert_eq!(loaded, Some(reconstruction));
    }

    #[test]
    fn reconstruction_returns_none_for_missing_file_id() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([99; 32]));
        let loaded =
            ReconstructionStore::reconstruction(&store, &file_id).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_reconstruction_returns_true_then_false() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([3; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let deleted = ReconstructionStore::delete_reconstruction(&store, &file_id)
            .expect("delete should succeed");
        assert!(deleted);
        let deleted_again = ReconstructionStore::delete_reconstruction(&store, &file_id)
            .expect("second delete should succeed");
        assert!(!deleted_again);
    }

    #[test]
    fn conditional_reconstruction_delete_preserves_replacement() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([4; 32]));
        let original = FileReconstruction::new(vec![]);
        let replacement = FileReconstruction::new(vec![ReconstructionTerm::new(
            StoredObjectId::new(ShardlineHash::from_bytes([5; 32])),
            ChunkRange::new(0, 1).unwrap(),
            1,
        )]);
        store.insert_reconstruction(&file_id, &original).unwrap();
        store.insert_reconstruction(&file_id, &replacement).unwrap();

        assert!(
            !ReconstructionStore::delete_reconstruction_if_matches(&store, &file_id, &original)
                .unwrap()
        );
        assert_eq!(
            ReconstructionStore::reconstruction(&store, &file_id).unwrap(),
            Some(replacement)
        );
    }

    #[test]
    fn list_reconstruction_file_ids_empty_initially() {
        let store = make_store();
        let ids =
            ReconstructionStore::list_reconstruction_file_ids(&store).expect("list should succeed");
        assert!(ids.is_empty());
    }

    #[test]
    fn list_reconstruction_file_ids_after_insert() {
        let store = make_store();
        let file_id = FileId::new(ShardlineHash::from_bytes([10; 32]));
        let reconstruction = FileReconstruction::new(vec![]);

        store
            .insert_reconstruction(&file_id, &reconstruction)
            .expect("insert should succeed");
        let ids =
            ReconstructionStore::list_reconstruction_file_ids(&store).expect("list should succeed");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], file_id);
    }

    #[test]
    fn insert_object_and_contains_object_roundtrip() {
        let store = make_store();
        let object_id = StoredObjectId::new(ShardlineHash::from_bytes([5; 32]));

        assert!(
            !ReconstructionStore::contains_object(&store, &object_id)
                .expect("check should succeed")
        );
        store
            .insert_object(&object_id)
            .expect("insert should succeed");
        assert!(
            ReconstructionStore::contains_object(&store, &object_id).expect("check should succeed")
        );
    }

    #[test]
    fn upsert_and_get_dedupe_shard_mapping_roundtrip() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([7; 32]);
        let object_key = ObjectKey::parse("shards/aa/test.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, object_key);

        store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert should succeed");
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, Some(mapping));
    }

    #[test]
    fn dedupe_shard_mapping_returns_none_for_missing_hash() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([99; 32]);
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_dedupe_shard_mapping_returns_true() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([8; 32]);
        let object_key = ObjectKey::parse("shards/bb/test.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, object_key);

        store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert should succeed");
        let deleted = DedupeStore::delete_dedupe_shard_mapping(&store, &chunk_hash)
            .expect("delete should succeed");
        assert!(deleted);
        let loaded =
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).expect("lookup should succeed");
        assert_eq!(loaded, None);
    }

    #[test]
    fn delete_dedupe_shard_mapping_returns_false_when_missing() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([99; 32]);
        let deleted = DedupeStore::delete_dedupe_shard_mapping(&store, &chunk_hash)
            .expect("delete should succeed");
        assert!(!deleted);
    }

    #[test]
    fn conditional_dedupe_delete_preserves_replacement() {
        let store = make_store();
        let chunk_hash = ShardlineHash::from_bytes([9; 32]);
        let original = DedupeShardMapping::new(
            chunk_hash,
            ObjectKey::parse("shards/cc/original.shard").unwrap(),
        );
        let replacement = DedupeShardMapping::new(
            chunk_hash,
            ObjectKey::parse("shards/cc/replacement.shard").unwrap(),
        );
        store.upsert_dedupe_shard_mapping(&original).unwrap();
        store.upsert_dedupe_shard_mapping(&replacement).unwrap();

        assert!(!DedupeStore::delete_dedupe_shard_mapping_if_matches(&store, &original).unwrap());
        assert_eq!(
            DedupeStore::dedupe_shard_mapping(&store, &chunk_hash).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: quarantine candidate ───────────────────────────────

    #[test]
    fn quarantine_candidate_returns_none_for_missing_key() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/missing").unwrap();
        let loaded =
            LifecycleStore::quarantine_candidate(&store, &key).expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn quarantine_candidate_upsert_and_read_roundtrip() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/test-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 100, 1000, 2000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        let loaded =
            LifecycleStore::quarantine_candidate(&store, &key).expect("lookup should succeed");
        assert_eq!(loaded, Some(candidate));
    }

    #[test]
    fn quarantine_candidate_list_includes_upserted() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/bb/list-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key, 200, 2000, 3000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        let candidates =
            LifecycleStore::list_quarantine_candidates(&store).expect("list should succeed");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].observed_length(), 200);
    }

    #[test]
    fn quarantine_candidate_delete_returns_true_then_false() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/del-candidate").unwrap();
        let candidate = QuarantineCandidate::new(key.clone(), 300, 3000, 4000).unwrap();

        LifecycleStore::upsert_quarantine_candidate(&store, &candidate)
            .expect("upsert should succeed");
        assert!(
            LifecycleStore::delete_quarantine_candidate(&store, &key)
                .expect("first delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_quarantine_candidate(&store, &key)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn quarantine_candidate_conditional_delete_preserves_replacement() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/conditional-candidate").unwrap();
        let original = QuarantineCandidate::new(key.clone(), 300, 3000, 4000).unwrap();
        let replacement = QuarantineCandidate::new(key, 301, 3000, 4000).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &original).unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &replacement).unwrap();

        assert!(
            !LifecycleStore::delete_quarantine_candidate_if_matches(&store, &original).unwrap()
        );
        assert_eq!(
            LifecycleStore::quarantine_candidate(&store, replacement.object_key()).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: retention hold ─────────────────────────────────────

    #[test]
    fn retention_hold_returns_none_for_missing_key() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/missing-hold").unwrap();
        let loaded = LifecycleStore::retention_hold(&store, &key).expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn retention_hold_upsert_and_read_roundtrip() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/aa/test-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "test reason".into(), 100, Some(200)).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        let loaded = LifecycleStore::retention_hold(&store, &key).expect("lookup should succeed");
        assert_eq!(loaded, Some(hold));
    }

    #[test]
    fn retention_hold_list_includes_upserted() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/bb/list-hold").unwrap();
        let hold = RetentionHold::new(key, "retain".into(), 300, None).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        let holds = LifecycleStore::list_retention_holds(&store).expect("list should succeed");
        assert_eq!(holds.len(), 1);
        assert_eq!(holds[0].reason(), "retain");
    }

    #[test]
    fn retention_hold_delete_returns_true_then_false() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/del-hold").unwrap();
        let hold = RetentionHold::new(key.clone(), "delete me".into(), 400, None).unwrap();

        LifecycleStore::upsert_retention_hold(&store, &hold).expect("upsert should succeed");
        assert!(
            LifecycleStore::delete_retention_hold(&store, &key)
                .expect("first delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_retention_hold(&store, &key)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn retention_hold_conditional_delete_preserves_replacement() {
        let store = make_store();
        let key = ObjectKey::parse("chunks/cc/conditional-hold").unwrap();
        let original =
            RetentionHold::new(key.clone(), "original".to_owned(), 3000, Some(4000)).unwrap();
        let replacement =
            RetentionHold::new(key, "replacement".to_owned(), 3000, Some(4000)).unwrap();
        LifecycleStore::upsert_retention_hold(&store, &original).unwrap();
        LifecycleStore::upsert_retention_hold(&store, &replacement).unwrap();

        assert!(!LifecycleStore::delete_retention_hold_if_matches(&store, &original).unwrap());
        assert_eq!(
            LifecycleStore::retention_hold(&store, replacement.object_key()).unwrap(),
            Some(replacement)
        );
    }

    // ── LifecycleStore: webhook delivery ───────────────────────────────────

    #[test]
    fn webhook_delivery_record_returns_true_for_new() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-1".into(),
            1000,
        )
        .unwrap();

        let recorded = LifecycleStore::record_webhook_delivery(&store, &delivery)
            .expect("record should succeed");
        assert!(recorded, "first record should return true");
    }

    #[test]
    fn webhook_delivery_record_returns_false_for_duplicate() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-dup".into(),
            1000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        let repeated = LifecycleStore::record_webhook_delivery(&store, &delivery)
            .expect("duplicate record should succeed");
        assert!(!repeated, "duplicate record should return false");
    }

    #[test]
    fn webhook_delivery_recreation_appends_without_rewriting_history() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-recreate".into(),
            1000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        assert!(LifecycleStore::delete_webhook_delivery(&store, &delivery).unwrap());

        let connection = store.open_connection().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_reliability_history_rewrites
                 BEFORE UPDATE ON shardline_reliability_events
                 BEGIN
                     SELECT RAISE(ABORT, 'reliability history was rewritten');
                 END;",
            )
            .unwrap();
        drop(connection);

        assert!(LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap());
    }

    #[test]
    fn webhook_delivery_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-tampered".into(),
            1000,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        let connection = store.open_connection().unwrap();
        let operation_id = webhook_snapshot(&delivery, WebhookDeliveryLifecycleState::Processed)
            .unwrap()
            .evidence_operation()
            .unwrap()
            .operation_id;
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\":99}'
                 WHERE operation_kind = 'WebhookDelivery' AND operation_id = ?1",
                rusqlite::params![operation_id],
            )
            .unwrap();

        assert!(LifecycleStore::list_webhook_deliveries(&store).is_err());
    }

    #[test]
    fn webhook_delivery_list_includes_recorded() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-list".into(),
            2000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        let deliveries =
            LifecycleStore::list_webhook_deliveries(&store).expect("list should succeed");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].delivery_id(), "delivery-list");
    }

    #[test]
    fn webhook_delivery_delete_returns_true_then_false() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-del".into(),
            3000,
        )
        .unwrap();

        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();
        assert!(
            LifecycleStore::delete_webhook_delivery(&store, &delivery)
                .expect("delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_webhook_delivery(&store, &delivery)
                .expect("second delete should succeed")
        );
    }

    #[test]
    fn webhook_delivery_conditional_delete_rejects_different_observation() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            "conditional-delivery".to_owned(),
            3000,
        )
        .unwrap();
        let different_observation = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            "conditional-delivery".to_owned(),
            3001,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        assert!(
            !LifecycleStore::delete_webhook_delivery_if_matches(&store, &different_observation)
                .unwrap()
        );
        assert_eq!(
            LifecycleStore::list_webhook_deliveries(&store).unwrap(),
            vec![delivery]
        );
    }

    #[test]
    fn webhook_delivery_purge_removes_only_rows_older_than_cutoff() {
        let store = make_store();
        let old = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-old".into(),
            100,
        )
        .unwrap();
        let fresh = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-fresh".into(),
            900,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &old).unwrap();
        LifecycleStore::record_webhook_delivery(&store, &fresh).unwrap();

        let purged = LifecycleStore::purge_webhook_deliveries_older_than(&store, 500)
            .expect("purge should succeed");
        assert_eq!(purged, 1, "only the row older than the cutoff is purged");
        let remaining = LifecycleStore::list_webhook_deliveries(&store).unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "rows inside the retention window must survive the purge"
        );
        assert_eq!(remaining[0].delivery_id(), "delivery-fresh");
        assert_eq!(remaining[0].processed_at_unix_seconds(), 900);
        // Dedup semantics stay intact: the surviving claim still dedups.
        assert!(
            !LifecycleStore::record_webhook_delivery(&store, &fresh).unwrap(),
            "surviving claim must still dedup after the purge"
        );
    }

    #[test]
    fn webhook_delivery_purge_is_idempotent_and_counts_rows() {
        let store = make_store();
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".into(),
            "repo".into(),
            "delivery-purge".into(),
            100,
        )
        .unwrap();
        LifecycleStore::record_webhook_delivery(&store, &delivery).unwrap();

        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            1
        );
        assert_eq!(
            LifecycleStore::purge_webhook_deliveries_older_than(&store, 200).unwrap(),
            0
        );
        assert!(
            LifecycleStore::list_webhook_deliveries(&store)
                .unwrap()
                .is_empty()
        );
    }

    // ── LifecycleStore: provider repository state ──────────────────────────

    #[test]
    fn provider_repository_state_returns_none_for_missing() {
        let store = make_store();
        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "no-owner",
            "no-repo",
        )
        .expect("lookup should succeed");
        assert!(loaded.is_none());
    }

    #[test]
    fn provider_repository_state_upsert_and_read_roundtrip() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "assets".into(),
            Some(100),
            Some(200),
            Some("refs/heads/main".into()),
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state)
            .expect("upsert should succeed");
        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "team",
            "assets",
        )
        .expect("lookup should succeed");
        assert_eq!(loaded, Some(state));
    }

    #[test]
    fn provider_repository_state_legacy_missing_evidence_remains_readable() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "legacy-provider".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:legacy-provider'",
                [],
            )
            .unwrap();
        assert_eq!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "legacy-provider",
            )
            .unwrap(),
            Some(state)
        );
    }

    #[test]
    fn provider_repository_state_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "tampered-provider".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:tampered-provider'",
                [],
            )
            .unwrap();
        assert!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-provider",
            )
            .is_err()
        );
    }

    #[test]
    fn provider_repository_state_delete_rejects_tampered_evidence() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "tampered-delete".into(),
            Some(100),
            None,
            None,
        );
        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'ProviderEvent'
                   AND operation_id = 'github:team:tampered-delete'",
                [],
            )
            .unwrap();
        assert!(
            LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-delete",
            )
            .is_err()
        );
        assert!(
            LifecycleStore::provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "tampered-delete",
            )
            .is_err()
        );
    }

    #[test]
    fn provider_repository_state_upsert_merges_partial_and_stale_observations() {
        let store = make_store();
        let revision = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            None,
            Some(200),
            Some("new-revision".into()),
        )
        .with_reconciliation(None, Some(180), None);
        let independent = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            Some(150),
            None,
            None,
        )
        .with_reconciliation(Some(170), None, Some(190));
        let stale = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "merge".into(),
            Some(100),
            Some(120),
            Some("stale-revision".into()),
        )
        .with_reconciliation(Some(110), Some(130), Some(140));

        for state in [&revision, &independent, &stale] {
            LifecycleStore::upsert_provider_repository_state(&store, state).unwrap();
        }

        let loaded = LifecycleStore::provider_repository_state(
            &store,
            RepositoryProvider::GitHub,
            "team",
            "merge",
        )
        .unwrap()
        .expect("merged state");
        assert_eq!(loaded.last_access_changed_at_unix_seconds(), Some(150));
        assert_eq!(loaded.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(loaded.last_pushed_revision(), Some("new-revision"));
        assert_eq!(loaded.last_cache_invalidated_at_unix_seconds(), Some(170));
        assert_eq!(
            loaded.last_authorization_rechecked_at_unix_seconds(),
            Some(180)
        );
        assert_eq!(loaded.last_drift_checked_at_unix_seconds(), Some(190));
    }

    #[test]
    fn provider_repository_state_list_includes_upserted() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "other".into(),
            Some(300),
            None,
            None,
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        let states = LifecycleStore::list_provider_repository_states(&store).unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].repo(), "other");
    }

    #[test]
    fn provider_repository_state_delete_returns_true_then_false() {
        let store = make_store();
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "del-repo".into(),
            None,
            None,
            None,
        );

        LifecycleStore::upsert_provider_repository_state(&store, &state).unwrap();
        assert!(
            LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "del-repo",
            )
            .expect("delete should succeed")
        );
        assert!(
            !LifecycleStore::delete_provider_repository_state(
                &store,
                RepositoryProvider::GitHub,
                "team",
                "del-repo",
            )
            .expect("second delete should succeed")
        );
    }

    // ── LifecycleStore: visit methods ──────────────────────────────────────

    #[test]
    fn visit_quarantine_candidates_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_quarantine_candidates(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_retention_holds_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_retention_holds(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_webhook_deliveries_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_webhook_deliveries(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_provider_repository_states_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        LifecycleStore::visit_provider_repository_states(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn visit_dedupe_shard_mappings_empty_store_does_not_call_visitor() {
        let store = make_store();
        let mut count = 0u32;
        DedupeStore::visit_dedupe_shard_mappings(&store, |_| {
            count += 1;
            Ok::<(), LocalIndexStoreError>(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    // ── UploadIntentStore: transition idempotency ─────────────────────────

    #[test]
    fn create_intent_rejects_id_reuse_for_different_object() {
        let store = make_store();
        let original = UploadIntent::new(
            "conflicting-intent".to_owned(),
            "objects/a".to_owned(),
            "hash-a".to_owned(),
            42,
        );
        let conflicting = UploadIntent::new(
            "conflicting-intent".to_owned(),
            "objects/b".to_owned(),
            "hash-b".to_owned(),
            43,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(store.create_intent(&original)).unwrap();
        runtime.block_on(store.create_intent(&original)).unwrap();
        assert!(matches!(
            runtime.block_on(store.create_intent(&conflicting)),
            Err(LocalIndexStoreError::UploadIntentConflict(_))
        ));
    }

    #[test]
    fn scoped_intent_baseline_and_transition_share_repository_identity() {
        let store = make_store();
        let intent = UploadIntent::new(
            "scoped-intent".to_owned(),
            "objects/scoped".to_owned(),
            "a".repeat(64),
            42,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(store.create_intent_scoped(&intent, "tenant-a", "repo-a"))
            .unwrap();
        runtime
            .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Storing))
            .unwrap();
        let events = runtime
            .block_on(store.reliability_events(intent.intent_id()))
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|stored_event| {
            stored_event.operation.tenant == "tenant-a"
                && stored_event.operation.repository == "repo-a"
        }));
    }

    #[test]
    fn sqlite_reliability_writer_rejects_tampered_event_before_insert() {
        let store = make_store();
        let mut event = upload_lifecycle_event(
            "tenant-a",
            "repo-a",
            "writer-integrity",
            "objects/integrity",
            "a".repeat(64),
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();
        event.state_digest = shardline_reliability::canonical_state_digest(&"tampered").unwrap();

        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        let result =
            crate::local_sqlite::helpers::persist_reliability_event_at(&transaction, &event, 0);
        assert!(result.is_err());
        assert_eq!(
            transaction
                .query_row(
                    "SELECT COUNT(*) FROM shardline_reliability_events
                     WHERE operation_id = ?1",
                    rusqlite::params!["writer-integrity"],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn sqlite_reliability_writer_rejects_conflicting_sequence_body() {
        use shardline_reliability::{LifecycleEvent, OperationIdentity, OperationKind};

        let store = make_store();
        let operation = OperationIdentity::new(
            "tenant-a",
            "repo-a",
            "writer-conflict",
            OperationKind::Upload,
        )
        .unwrap()
        .with_object_key("objects/first")
        .with_content_sha256("a".repeat(64));
        let first = LifecycleEvent::new(
            operation.clone(),
            0,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();
        let conflicting = LifecycleEvent::new(
            operation.with_object_key("objects/second"),
            0,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )
        .unwrap();

        let mut connection = store.open_connection().unwrap();
        let transaction = connection.transaction().unwrap();
        crate::local_sqlite::helpers::persist_reliability_event_at(&transaction, &first, 0)
            .unwrap();
        assert!(matches!(
            crate::local_sqlite::helpers::persist_reliability_event_at(
                &transaction,
                &conflicting,
                0,
            ),
            Err(LocalIndexStoreError::ReliabilityEventConflict(operation_id))
                if operation_id == "writer-conflict"
        ));
    }

    #[test]
    fn transition_intent_to_same_state_is_idempotent() {
        let store = make_store();
        let intent = UploadIntent::new(
            "same-state-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();

        // Advance Created -> Storing.
        let first = rt
            .block_on(store.transition_intent("same-state-intent", UploadIntentState::Storing))
            .unwrap();
        assert!(first, "initial Created -> Storing should succeed");
        // Repeating the same transition (a duplicate concurrent caller's view)
        // must be a no-op success, not a false/invalid transition.
        let second = rt
            .block_on(store.transition_intent("same-state-intent", UploadIntentState::Storing))
            .unwrap();
        assert!(second, "same-state transition must be idempotent");
    }

    #[test]
    fn upload_intent_read_rejects_missing_evidence_without_writing() {
        let store = make_store();
        let intent = UploadIntent::new(
            "repair-upload-evidence".to_owned(),
            "objects/repair-upload-evidence".to_owned(),
            "d".repeat(64),
            42,
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(store.create_intent(&intent)).unwrap();
        assert!(
            runtime
                .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Storing))
                .unwrap()
        );
        assert!(
            runtime
                .block_on(store.transition_intent(intent.intent_id(), UploadIntentState::Stored))
                .unwrap()
        );
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'Upload' AND operation_id = ?1",
                params![intent.intent_id()],
            )
            .unwrap();
        drop(connection);

        assert!(
            runtime
                .block_on(store.intent_by_id(intent.intent_id()))
                .is_err()
        );
        assert!(
            runtime
                .block_on(store.reliability_events(intent.intent_id()))
                .is_err()
        );
    }

    #[test]
    fn transition_with_reliability_event_is_atomic_and_verifiable() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, verify_lifecycle_chain,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-intent".to_owned(),
            "objects/reliability".to_owned(),
            "a".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let event = LifecycleEvent::new(
            OperationIdentity::new(
                "shardline",
                "default",
                intent.intent_id(),
                OperationKind::Upload,
            )
            .unwrap()
            .with_object_key(intent.object_key())
            .with_content_sha256(intent.object_hash()),
            1,
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Storing,
        )
        .unwrap();

        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &event,
            ))
            .unwrap()
        );
        let events = rt
            .block_on(store.reliability_events(intent.intent_id()))
            .unwrap();
        verify_lifecycle_chain(&events).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events.last(), Some(&event));
        assert_eq!(
            rt.block_on(store.intent_by_id(intent.intent_id()))
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
    }

    #[test]
    fn conflicting_reliability_evidence_rolls_back_the_state_transition() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, UploadLifecycleState,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-conflict".to_owned(),
            "objects/reliability-conflict".to_owned(),
            "b".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let operation = OperationIdentity::new(
            "shardline",
            "default",
            intent.intent_id(),
            OperationKind::Upload,
        )
        .unwrap()
        .with_object_key(intent.object_key())
        .with_content_sha256(intent.object_hash());
        let first = LifecycleEvent::new(
            operation.clone(),
            1,
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &first,
            ))
            .unwrap()
        );

        let conflicting = LifecycleEvent::new(
            operation.with_object_key("objects/different"),
            1,
            UploadLifecycleState::Storing,
            UploadLifecycleState::Stored,
        )
        .unwrap();
        assert!(matches!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Stored,
                &conflicting,
            )),
            Err(LocalIndexStoreError::ReliabilityEventConflict(_))
        ));
        assert_eq!(
            rt.block_on(store.intent_by_id(intent.intent_id()))
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
    }

    #[test]
    fn tampered_reliability_evidence_is_rejected_on_read() {
        use shardline_reliability::{
            LifecycleEvent, OperationIdentity, OperationKind, UploadLifecycleState,
        };

        let store = make_store();
        let intent = UploadIntent::new(
            "reliability-tamper".to_owned(),
            "objects/reliability-tamper".to_owned(),
            "c".repeat(64),
            42,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();
        let event = LifecycleEvent::new(
            OperationIdentity::new(
                "shardline",
                "default",
                intent.intent_id(),
                OperationKind::Upload,
            )
            .unwrap()
            .with_object_key(intent.object_key())
            .with_content_sha256(intent.object_hash()),
            1,
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        assert!(
            rt.block_on(store.transition_intent_with_event(
                intent.intent_id(),
                UploadIntentState::Storing,
                &event,
            ))
            .unwrap()
        );

        let connection = store.open_connection().unwrap();
        let mut tampered: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();
        tampered["after"] = serde_json::Value::String("Stored".to_owned());
        connection
            .execute(
                "UPDATE shardline_reliability_events SET event_json = ?1
                 WHERE operation_kind = 'Upload' AND operation_id = ?2 AND sequence = 1",
                rusqlite::params![tampered.to_string(), intent.intent_id()],
            )
            .unwrap();

        assert!(matches!(
            rt.block_on(store.reliability_events(intent.intent_id())),
            Err(LocalIndexStoreError::Reliability(_))
        ));
    }

    #[test]
    fn concurrent_same_intent_transitions_both_succeed() {
        use std::sync::Arc;

        let store = make_store();
        let intent = UploadIntent::new(
            "concurrent-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(store.create_intent(&intent)).unwrap();

        // Two callers racing to move the same intent into Storing. Both must end
        // up observing success (a spurious InvalidUploadTransition is a bug).
        let store = Arc::new(store);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let handle = rt.handle().clone();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let handle = handle.clone();
            handles.push(std::thread::spawn(move || {
                handle.block_on(async {
                    barrier.wait().await;
                    store
                        .transition_intent("concurrent-intent", UploadIntentState::Storing)
                        .await
                        .unwrap()
                })
            }));
        }
        for h in handles {
            assert!(
                h.join().unwrap(),
                "a concurrent same-intent transition must not fail"
            );
        }
    }

    #[test]
    fn quarantine_candidate_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let candidate = QuarantineCandidate::new(
            ObjectKey::parse("aa/quarantine-object").unwrap(),
            42,
            100,
            200,
        )
        .unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'GarbageCollection'
                   AND operation_id = 'aa/quarantine-object'",
                [],
            )
            .unwrap();
        assert!(LifecycleStore::quarantine_candidate(&store, candidate.object_key()).is_err());
    }

    #[test]
    fn retention_hold_tampered_evidence_is_rejected_on_read() {
        let store = make_store();
        let hold = RetentionHold::new(
            ObjectKey::parse("aa/retention-object").unwrap(),
            "legal hold".to_owned(),
            100,
            Some(200),
        )
        .unwrap();
        LifecycleStore::upsert_retention_hold(&store, &hold).unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'RetentionHold'
                   AND operation_id = 'aa/retention-object'",
                [],
            )
            .unwrap();
        assert!(LifecycleStore::retention_hold(&store, hold.object_key()).is_err());
    }

    #[test]
    fn quarantine_delete_repairs_missing_baseline_chain() {
        let store = make_store();
        let candidate = QuarantineCandidate::new(
            ObjectKey::parse("aa/quarantine-repair").unwrap(),
            42,
            100,
            200,
        )
        .unwrap();
        LifecycleStore::upsert_quarantine_candidate(&store, &candidate).unwrap();
        {
            let connection = store.open_connection().unwrap();
            connection
                .execute(
                    "DELETE FROM shardline_reliability_events
                     WHERE operation_kind = 'GarbageCollection'
                       AND operation_id = 'aa/quarantine-repair'",
                    [],
                )
                .unwrap();
        }

        assert!(
            LifecycleStore::delete_quarantine_candidate(&store, candidate.object_key()).unwrap()
        );
        let connection = store.open_connection().unwrap();
        let (count, minimum, maximum): (i64, i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), MIN(sequence), MAX(sequence)
                 FROM shardline_reliability_events
                 WHERE operation_kind = 'GarbageCollection'
                   AND operation_id = 'aa/quarantine-repair'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((count, minimum, maximum), (2, 0, 1));
    }
}
