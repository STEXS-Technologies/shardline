use rusqlite::{OptionalExtension, params};
use shardline_reliability::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleState, OciObjectSnapshot,
    verify_oci_object_lifecycle_chain,
};

use super::{LocalIndexStore, LocalIndexStoreError, i64_to_u64};
use crate::{OciObjectKey, OciObjectKind, OciObjectStore, OciObjectTombstone, OciTagEntry};

fn oci_identity(key: &OciObjectKey) -> Result<OciObjectIdentity, LocalIndexStoreError> {
    Ok(OciObjectIdentity::new(
        key.scope_namespace.clone(),
        key.repository.clone(),
        key.kind.as_str(),
        key.digest_hex.clone(),
    )?)
}

fn oci_snapshot(
    key: &OciObjectKey,
    state: OciObjectLifecycleState,
    deleted_at: Option<u64>,
) -> Result<OciObjectSnapshot, LocalIndexStoreError> {
    Ok(OciObjectSnapshot::new(
        oci_identity(key)?,
        state,
        deleted_at,
    )?)
}

fn load_oci_evidence(
    transaction: &rusqlite::Transaction<'_>,
    key: &OciObjectKey,
) -> Result<OciObjectEvidenceLog, LocalIndexStoreError> {
    let operation_id = format!(
        "{}:{}:{}:{}",
        key.scope_namespace,
        key.repository,
        key.kind.as_str(),
        key.digest_hex
    );
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'Visibility' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![operation_id], |row| {
        let json: String = row.get(0)?;
        serde_json::from_str(json.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })?;
    Ok(OciObjectEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn persist_oci_evidence(
    transaction: &rusqlite::Transaction<'_>,
    event: &shardline_reliability::OciObjectLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    let created_at_unix_seconds =
        transaction.query_row("SELECT unixepoch()", [], |row| row.get(0))?;
    super::helpers::persist_reliability_event_at(transaction, event, created_at_unix_seconds)
}

fn record_oci_evidence(
    transaction: &rusqlite::Transaction<'_>,
    key: &OciObjectKey,
    state: OciObjectLifecycleState,
    deleted_at: Option<u64>,
    fallback_state: OciObjectLifecycleState,
    fallback_deleted_at: Option<u64>,
) -> Result<(), LocalIndexStoreError> {
    let after = oci_snapshot(key, state, deleted_at)?;
    let mut evidence = load_oci_evidence(transaction, key)?;
    let evidence_was_empty = evidence.events().is_empty();
    if evidence.events().is_empty() {
        evidence = OciObjectEvidenceLog::baseline(oci_snapshot(
            key,
            fallback_state,
            fallback_deleted_at,
        )?)?;
    }
    evidence.record(after)?;
    let event = evidence.events().last().ok_or_else(|| {
        LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
            "OCI object evidence event",
        ))
    })?;
    if evidence_was_empty {
        for stored_event in evidence.events() {
            persist_oci_evidence(transaction, stored_event)?;
        }
        Ok(())
    } else {
        persist_oci_evidence(transaction, event)
    }
}

impl LocalIndexStore {
    fn list_oci_object_tombstones_blocking(
        &self,
    ) -> Result<Vec<OciObjectTombstone>, LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT scope_namespace, repository, object_kind, digest_hex,
                    deleted_at_unix_seconds
             FROM shardline_oci_object_tombstones
             ORDER BY scope_namespace, repository, object_kind, digest_hex",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut tombstones = Vec::new();
        for row in rows {
            let (scope_namespace, repository, object_kind, digest_hex, deleted_at) = row?;
            let kind = object_kind.parse()?;
            tombstones.push(OciObjectTombstone {
                key: OciObjectKey {
                    scope_namespace,
                    repository,
                    kind,
                    digest_hex,
                },
                deleted_at_unix_seconds: i64_to_u64(deleted_at)?,
            });
        }
        drop(statement);
        for tombstone in &tombstones {
            let evidence = load_oci_evidence(&transaction, &tombstone.key)?;
            let expected = oci_snapshot(
                &tombstone.key,
                OciObjectLifecycleState::Deleted,
                Some(tombstone.deleted_at_unix_seconds),
            )?;
            if evidence.events().is_empty() {
                let baseline = OciObjectEvidenceLog::baseline(expected.clone())?;
                baseline.verify_for(&expected)?;
                for event in baseline.events() {
                    persist_oci_evidence(&transaction, event)?;
                }
            } else {
                evidence.verify_for(&expected)?;
            }
        }
        transaction.commit()?;
        Ok(tombstones)
    }

    fn publish_oci_object_blocking(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let previous_deleted_at = transaction
            .query_row(
                "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
                 WHERE scope_namespace = ?1 AND repository = ?2 AND object_kind = ?3 AND digest_hex = ?4",
                params![key.scope_namespace, key.repository, key.kind.as_str(), key.digest_hex],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(i64_to_u64)
            .transpose()?;
        transaction.execute(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = ?1 AND repository = ?2
               AND object_kind = ?3 AND digest_hex = ?4",
            params![
                key.scope_namespace,
                key.repository,
                key.kind.as_str(),
                key.digest_hex
            ],
        )?;
        for tag in tags {
            transaction.execute(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = excluded.digest_hex",
                params![tag.scope_namespace, tag.repository, tag.tag, tag.digest_hex],
            )?;
        }
        record_oci_evidence(
            &transaction,
            key,
            OciObjectLifecycleState::Published,
            None,
            if previous_deleted_at.is_some() {
                OciObjectLifecycleState::Deleted
            } else {
                OciObjectLifecycleState::Published
            },
            previous_deleted_at,
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn delete_oci_object_blocking(&self, key: &OciObjectKey) -> Result<(), LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
        let previous_deleted_at = transaction
            .query_row(
                "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
                 WHERE scope_namespace = ?1 AND repository = ?2 AND object_kind = ?3 AND digest_hex = ?4",
                params![key.scope_namespace, key.repository, key.kind.as_str(), key.digest_hex],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(i64_to_u64)
            .transpose()?;
        transaction.execute(
            "INSERT INTO shardline_oci_object_tombstones
                (scope_namespace, repository, object_kind, digest_hex, deleted_at_unix_seconds)
             VALUES (?1, ?2, ?3, ?4, unixepoch())
             ON CONFLICT (scope_namespace, repository, object_kind, digest_hex)
             DO UPDATE SET deleted_at_unix_seconds = excluded.deleted_at_unix_seconds",
            params![
                key.scope_namespace,
                key.repository,
                key.kind.as_str(),
                key.digest_hex
            ],
        )?;
        if key.kind == OciObjectKind::Manifest {
            transaction.execute(
                "DELETE FROM shardline_oci_tags
                 WHERE scope_namespace = ?1 AND repository = ?2 AND digest_hex = ?3",
                params![key.scope_namespace, key.repository, key.digest_hex],
            )?;
        }
        let deleted_at = transaction.query_row(
            "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
             WHERE scope_namespace = ?1 AND repository = ?2 AND object_kind = ?3 AND digest_hex = ?4",
            params![key.scope_namespace, key.repository, key.kind.as_str(), key.digest_hex],
            |row| row.get::<_, i64>(0),
        )
        .map(i64_to_u64)??;
        record_oci_evidence(
            &transaction,
            key,
            OciObjectLifecycleState::Deleted,
            Some(deleted_at),
            if previous_deleted_at.is_some() {
                OciObjectLifecycleState::Deleted
            } else {
                OciObjectLifecycleState::Published
            },
            previous_deleted_at,
        )?;
        transaction.commit()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl OciObjectStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn oci_object_is_deleted(&self, key: &OciObjectKey) -> Result<bool, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = store.open_connection()?;
            let transaction = connection.transaction()?;
            let deleted_at = transaction
                .query_row(
                    "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
                     WHERE scope_namespace = ?1 AND repository = ?2
                       AND object_kind = ?3 AND digest_hex = ?4",
                    params![
                        key.scope_namespace,
                        key.repository,
                        key.kind.as_str(),
                        key.digest_hex
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .map(i64_to_u64)
                .transpose()?;
            let evidence = load_oci_evidence(&transaction, &key)?;
            if evidence.events().is_empty() {
                if let Some(deleted_at) = deleted_at {
                    let expected =
                        oci_snapshot(&key, OciObjectLifecycleState::Deleted, Some(deleted_at))?;
                    let baseline = OciObjectEvidenceLog::baseline(expected.clone())?;
                    baseline.verify_for(&expected)?;
                    for event in baseline.events() {
                        persist_oci_evidence(&transaction, event)?;
                    }
                }
            } else {
                verify_oci_object_lifecycle_chain(evidence.events())?;
                if let Some(deleted_at) = deleted_at {
                    let expected =
                        oci_snapshot(&key, OciObjectLifecycleState::Deleted, Some(deleted_at))?;
                    evidence.verify_for(&expected)?;
                }
            }
            transaction.commit()?;
            Ok(deleted_at.is_some())
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn publish_oci_object(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let tags = tags.to_vec();
        tokio::task::spawn_blocking(move || store.publish_oci_object_blocking(&key, &tags))
            .await
            .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn delete_oci_object(&self, key: &OciObjectKey) -> Result<(), Self::Error> {
        let store = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || store.delete_oci_object_blocking(&key))
            .await
            .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn list_oci_object_tombstones(&self) -> Result<Vec<OciObjectTombstone>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.list_oci_object_tombstones_blocking())
            .await
            .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn delete_oci_object_tombstone_if_unchanged(
        &self,
        tombstone: &OciObjectTombstone,
    ) -> Result<bool, Self::Error> {
        let store = self.clone();
        let tombstone = tombstone.clone();
        tokio::task::spawn_blocking(move || {
            let deleted_at = i64::try_from(tombstone.deleted_at_unix_seconds)
                .map_err(|error| LocalIndexStoreError::IntegerOutOfRange(error.to_string()))?;
            let mut connection = store.open_connection()?;
            let transaction = connection.transaction()?;
            let found = transaction
                .query_row(
                    "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
                     WHERE scope_namespace = ?1 AND repository = ?2
                       AND object_kind = ?3 AND digest_hex = ?4
                       AND deleted_at_unix_seconds = ?5",
                    params![
                        tombstone.key.scope_namespace,
                        tombstone.key.repository,
                        tombstone.key.kind.as_str(),
                        tombstone.key.digest_hex,
                        deleted_at,
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            let deleted = transaction.execute(
                "DELETE FROM shardline_oci_object_tombstones
                 WHERE scope_namespace = ?1 AND repository = ?2
                   AND object_kind = ?3 AND digest_hex = ?4
                   AND deleted_at_unix_seconds = ?5",
                params![
                    tombstone.key.scope_namespace,
                    tombstone.key.repository,
                    tombstone.key.kind.as_str(),
                    tombstone.key.digest_hex,
                    deleted_at,
                ],
            )?;
            if found.is_some() {
                record_oci_evidence(
                    &transaction,
                    &tombstone.key,
                    OciObjectLifecycleState::Reclaimed,
                    Some(tombstone.deleted_at_unix_seconds),
                    OciObjectLifecycleState::Deleted,
                    Some(tombstone.deleted_at_unix_seconds),
                )?;
            }
            transaction.commit()?;
            Ok(deleted != 0)
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::OciTagStore as _;

    fn object(kind: OciObjectKind, digest: char) -> OciObjectKey {
        OciObjectKey {
            scope_namespace: "oci-tombstone-local".to_owned(),
            repository: "team/assets".to_owned(),
            kind,
            digest_hex: digest.to_string().repeat(64),
        }
    }

    fn tag(name: &str, digest: char) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-tombstone-local".to_owned(),
            repository: "team/assets".to_owned(),
            tag: name.to_owned(),
            digest_hex: digest.to_string().repeat(64),
        }
    }

    #[tokio::test]
    async fn manifest_delete_and_republish_are_atomic_metadata_commits() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let manifest = object(OciObjectKind::Manifest, 'a');
        let current = tag("latest", 'a');
        let unrelated = tag("stable", 'b');

        store
            .publish_oci_object(&manifest, &[current.clone(), unrelated.clone()])
            .await
            .unwrap();
        store.delete_oci_object(&manifest).await.unwrap();

        assert!(store.oci_object_is_deleted(&manifest).await.unwrap());
        assert!(
            store
                .oci_tag(&current.scope_namespace, &current.repository, &current.tag)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .oci_tag(
                    &unrelated.scope_namespace,
                    &unrelated.repository,
                    &unrelated.tag,
                )
                .await
                .unwrap(),
            Some(unrelated)
        );

        store
            .publish_oci_object(&manifest, std::slice::from_ref(&current))
            .await
            .unwrap();
        assert!(!store.oci_object_is_deleted(&manifest).await.unwrap());
        assert_eq!(
            store
                .oci_tag(&current.scope_namespace, &current.repository, &current.tag)
                .await
                .unwrap(),
            Some(current)
        );
    }

    #[tokio::test]
    async fn blob_tombstone_does_not_mutate_manifest_tags() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let blob = object(OciObjectKind::Blob, 'a');
        let current = tag("latest", 'a');
        store.upsert_oci_tag(&current).await.unwrap();

        store.delete_oci_object(&blob).await.unwrap();

        assert!(store.oci_object_is_deleted(&blob).await.unwrap());
        assert_eq!(
            store
                .oci_tag(&current.scope_namespace, &current.repository, &current.tag)
                .await
                .unwrap(),
            Some(current)
        );
    }

    #[tokio::test]
    async fn tombstone_inventory_and_generation_compare_delete() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let blob = object(OciObjectKind::Blob, 'c');
        store.delete_oci_object(&blob).await.unwrap();

        let tombstones = store.list_oci_object_tombstones().await.unwrap();
        assert_eq!(tombstones.len(), 1);
        let tombstone = tombstones.first().unwrap().clone();
        assert_eq!(tombstone.key, blob);

        let mut stale = tombstone.clone();
        stale.deleted_at_unix_seconds = stale.deleted_at_unix_seconds.saturating_add(1);
        assert!(
            !store
                .delete_oci_object_tombstone_if_unchanged(&stale)
                .await
                .unwrap()
        );
        assert!(store.oci_object_is_deleted(&blob).await.unwrap());
        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
        assert!(!store.oci_object_is_deleted(&blob).await.unwrap());
    }

    #[tokio::test]
    async fn tampered_tombstone_evidence_is_rejected_on_read() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let blob = object(OciObjectKind::Blob, 'e');
        store.delete_oci_object(&blob).await.unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"sequence\": 99}'
                 WHERE operation_kind = 'Visibility'",
                [],
            )
            .unwrap();
        assert!(store.oci_object_is_deleted(&blob).await.is_err());
    }

    #[tokio::test]
    async fn reclaim_repairs_missing_visibility_baseline_chain() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let blob = object(OciObjectKind::Blob, 'f');
        store.delete_oci_object(&blob).await.unwrap();
        let tombstone = store
            .list_oci_object_tombstones()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.key == blob)
            .unwrap();
        {
            let connection = store.open_connection().unwrap();
            connection
                .execute(
                    "DELETE FROM shardline_reliability_events
                     WHERE operation_kind = 'Visibility'",
                    [],
                )
                .unwrap();
        }

        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
        let connection = store.open_connection().unwrap();
        let (count, minimum, maximum): (i64, i64, i64) = connection
            .query_row(
                "SELECT COUNT(*), MIN(sequence), MAX(sequence)
                 FROM shardline_reliability_events
                 WHERE operation_kind = 'Visibility'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((count, minimum, maximum), (2, 0, 1));
    }
}
