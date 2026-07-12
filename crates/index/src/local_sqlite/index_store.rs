use rusqlite::{OptionalExtension, params};
use shardline_protocol::{RepositoryProvider, ShardlineHash, unix_now_seconds_lossy};
use shardline_storage::ObjectKey;

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, u64_to_i64};
use crate::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LifecycleStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionStore, RetentionHold,
    StoredObjectId, WebhookDelivery, xet_hash_hex_string,
};

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
            let hash = crate::parse_xet_hash_hex(&row?)?;
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
        connection
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = ?1",
                params![xet_hash_hex_string(chunk_hash)],
                super::helpers::dedupe_shard_mapping_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
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
}

impl LifecycleStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        let connection = self.open_connection()?;
        connection
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
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
             FROM shardline_quarantine_candidates
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::quarantine_candidate_from_row)?;
        collect_rows(rows)
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
        let connection = self.open_connection()?;
        connection.execute(
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
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_quarantine_candidates WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        Ok(changed > 0)
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        let connection = self.open_connection()?;
        connection
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
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT object_key,
                    reason,
                    held_at_unix_seconds,
                    release_after_unix_seconds
             FROM shardline_retention_holds
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], super::helpers::retention_hold_from_row)?;
        collect_rows(rows)
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
        let connection = self.open_connection()?;
        connection.execute(
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
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_retention_holds WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        Ok(changed > 0)
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
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
        Ok(changed > 0)
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    delivery_id,
                    processed_at_unix_seconds
             FROM shardline_webhook_deliveries
             ORDER BY provider, owner, repo, delivery_id",
        )?;
        let rows = statement.query_map([], super::helpers::webhook_delivery_from_row)?;
        collect_rows(rows)
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
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
            ],
        )?;
        Ok(changed > 0)
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        let connection = self.open_connection()?;
        connection
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
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let connection = self.open_connection()?;
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
        let rows = statement.query_map([], super::helpers::provider_repository_state_from_row)?;
        collect_rows(rows)
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
        let connection = self.open_connection()?;
        let now = unix_now_seconds_lossy();
        connection.execute(
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
                last_access_changed_at_unix_seconds =
                    excluded.last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds =
                    excluded.last_revision_pushed_at_unix_seconds,
                last_pushed_revision = excluded.last_pushed_revision,
                last_cache_invalidated_at_unix_seconds =
                    excluded.last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds =
                    excluded.last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds =
                    excluded.last_drift_checked_at_unix_seconds,
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
        Ok(())
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![provider.as_str(), owner, repo],
        )?;
        Ok(changed > 0)
    }
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{ChunkRange, RepositoryProvider};
    use shardline_storage::ObjectKey;

    use super::*;
    use crate::ReconstructionTerm;

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
}
