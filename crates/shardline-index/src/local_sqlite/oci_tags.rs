use std::collections::HashMap;

use rusqlite::{OptionalExtension, Transaction, params};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows};
use crate::{
    OciTagEntry, OciTagStore,
    local_sqlite::{
        current_oci_tag_evidence, oci_tag_snapshot, persist_oci_tag_evidence,
        verify_oci_tag_evidence,
    },
};
use shardline_reliability::{
    OciTagLifecycleEvent, OperationKind, SnapshotEvidence, persisted_event_sequence,
    verify_and_append_snapshot_transition, verify_persisted_merkle_commit_with_previous,
    verify_snapshot_event,
};

pub(crate) fn current_tag(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
) -> Result<Option<OciTagEntry>, LocalIndexStoreError> {
    Ok(transaction
        .query_row(
            "SELECT scope_namespace, repository, tag, digest_hex
             FROM shardline_oci_tags
             WHERE scope_namespace = ?1 AND repository = ?2 AND tag = ?3",
            params![scope_namespace, repository, tag],
            entry_from_row,
        )
        .optional()?)
}

pub(crate) fn record_tag_transition(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    before: Option<String>,
    after: Option<String>,
) -> Result<(), LocalIndexStoreError> {
    let before_snapshot = oci_tag_snapshot(scope_namespace, repository, tag, before.clone())?;
    let after_snapshot = oci_tag_snapshot(scope_namespace, repository, tag, after)?;
    let (stored_evidence, evidence_was_missing) =
        current_oci_tag_evidence(transaction, scope_namespace, repository, tag, before)?;
    let evidence =
        verify_and_append_snapshot_transition(stored_evidence, before_snapshot, after_snapshot)?.0;
    if evidence_was_missing {
        for event in evidence.events() {
            persist_oci_tag_evidence(transaction, event)?;
        }
    } else if let Some(event) = evidence.events().last() {
        persist_oci_tag_evidence(transaction, event)?;
    }
    Ok(())
}

fn entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OciTagEntry> {
    Ok(OciTagEntry {
        scope_namespace: row.get("scope_namespace")?,
        repository: row.get("repository")?,
        tag: row.get("tag")?,
        digest_hex: row.get("digest_hex")?,
    })
}

fn verify_oci_tag_listing_evidence(
    transaction: &Transaction<'_>,
    values: &[OciTagEntry],
) -> Result<(), LocalIndexStoreError> {
    if values.is_empty() {
        return Ok(());
    }
    let mut operations = Vec::with_capacity(values.len());
    for value in values {
        let snapshot = oci_tag_snapshot(
            &value.scope_namespace,
            &value.repository,
            &value.tag,
            Some(value.digest_hex.clone()),
        )?;
        operations.push(snapshot.evidence_operation()?);
    }
    let operation_ids = operations
        .iter()
        .map(|operation| operation.operation_id.clone())
        .collect::<Vec<_>>();
    let placeholders = (0..operation_ids.len())
        .map(|index| format!("?{}", index.saturating_add(2)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT current.operation_id, current.sequence, current.event_json,
                current.merkle_commit_json,
                (SELECT previous.merkle_commit_json
                 FROM shardline_reliability_events AS previous
                 WHERE previous.operation_kind = ?1
                   AND previous.operation_id = current.operation_id
                   AND previous.sequence < current.sequence
                   AND previous.merkle_commit_json IS NOT NULL
                 ORDER BY previous.sequence DESC LIMIT 1) AS previous_merkle_json
         FROM shardline_reliability_events AS current
         WHERE current.operation_kind = ?1
           AND current.operation_id IN ({placeholders})
           AND current.sequence = (
               SELECT MAX(latest.sequence)
               FROM shardline_reliability_events AS latest
               WHERE latest.operation_kind = ?1
                 AND latest.operation_id = current.operation_id
           )"
    );
    let mut parameters = Vec::with_capacity(operation_ids.len().saturating_add(1));
    parameters.push(OperationKind::OciTag.as_str().to_owned());
    parameters.extend(operation_ids.iter().cloned());
    let mut statement = transaction.prepare(&sql)?;
    let rows = statement.query_map(rusqlite::params_from_iter(parameters.iter()), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;
    let mut latest = HashMap::with_capacity(values.len());
    for row in rows {
        let (operation_id, sequence, event_json, merkle_json, previous_json) = row?;
        latest.insert(
            operation_id,
            (
                sequence,
                serde_json::from_str::<serde_json::Value>(&event_json)?,
                merkle_json
                    .map(|json| serde_json::from_str::<serde_json::Value>(&json))
                    .transpose()?,
                previous_json
                    .map(|json| serde_json::from_str::<serde_json::Value>(&json))
                    .transpose()?,
            ),
        );
    }
    for (value, operation) in values.iter().zip(operations) {
        let Some((row_sequence, event_json, merkle_json, previous_json)) =
            latest.remove(&operation.operation_id)
        else {
            return Err(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ));
        };
        let event_sequence = persisted_event_sequence(OperationKind::OciTag, event_json.clone())?;
        if u64::try_from(row_sequence).ok() != Some(event_sequence) {
            return Err(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::Merkle(
                    "OCI tag listing evidence sequence mismatch".into(),
                ),
            ));
        }
        verify_persisted_merkle_commit_with_previous(
            OperationKind::OciTag,
            event_json.clone(),
            merkle_json,
            previous_json,
        )?;
        let event: OciTagLifecycleEvent = serde_json::from_value(event_json)?;
        let expected = oci_tag_snapshot(
            &value.scope_namespace,
            &value.repository,
            &value.tag,
            Some(value.digest_hex.clone()),
        )?;
        verify_snapshot_event(&event, &expected)?;
        if event.operation != operation {
            return Err(LocalIndexStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ));
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl OciTagStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn upsert_oci_tag(&self, entry: &OciTagEntry) -> Result<(), Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let before = current_tag(
                    &transaction,
                    &entry.scope_namespace,
                    &entry.repository,
                    &entry.tag,
                )?;
                transaction.execute(
                    "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (scope_namespace, repository, tag)
                     DO UPDATE SET digest_hex = excluded.digest_hex",
                    params![
                        entry.scope_namespace,
                        entry.repository,
                        entry.tag,
                        entry.digest_hex
                    ],
                )?;
                record_tag_transition(
                    &transaction,
                    &entry.scope_namespace,
                    &entry.repository,
                    &entry.tag,
                    before.map(|value| value.digest_hex),
                    Some(entry.digest_hex.clone()),
                )?;
                transaction.commit()?;
                Ok(())
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let changed = transaction.execute(
                    "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (scope_namespace, repository, tag) DO NOTHING",
                    params![
                        entry.scope_namespace,
                        entry.repository,
                        entry.tag,
                        entry.digest_hex
                    ],
                )?;
                if changed == 1 {
                    record_tag_transition(
                        &transaction,
                        &entry.scope_namespace,
                        &entry.repository,
                        &entry.tag,
                        None,
                        Some(entry.digest_hex.clone()),
                    )?;
                } else if let Some(current) = current_tag(
                    &transaction,
                    &entry.scope_namespace,
                    &entry.repository,
                    &entry.tag,
                )? {
                    verify_oci_tag_evidence(
                        &transaction,
                        &current.scope_namespace,
                        &current.repository,
                        &current.tag,
                        Some(current.digest_hex),
                    )?;
                }
                transaction.commit()?;
                Ok(changed == 1)
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let repository = repository.to_owned();
        let tag = tag.to_owned();
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let value = current_tag(&transaction, &scope_namespace, &repository, &tag)?;
                verify_oci_tag_evidence(
                    &transaction,
                    &scope_namespace,
                    &repository,
                    &tag,
                    value.as_ref().map(|entry| entry.digest_hex.clone()),
                )?;
                transaction.commit()?;
                Ok(value)
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let repository = repository.to_owned();
        let cursor = cursor.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let limit = i64::try_from(limit)
                    .map_err(|error| LocalIndexStoreError::IntegerOutOfRange(error.to_string()))?;
                let values =
                    if let Some(cursor) = cursor.as_deref() {
                        let mut statement = transaction.prepare(
                            "SELECT scope_namespace, repository, tag, digest_hex
                         FROM shardline_oci_tags
                         WHERE scope_namespace = ?1 AND repository = ?2 AND tag > ?3
                         ORDER BY tag LIMIT ?4",
                        )?;
                        collect_rows(statement.query_map(
                            params![scope_namespace, repository, cursor, limit],
                            entry_from_row,
                        )?)?
                    } else {
                        let mut statement = transaction.prepare(
                            "SELECT scope_namespace, repository, tag, digest_hex
                         FROM shardline_oci_tags
                         WHERE scope_namespace = ?1 AND repository = ?2
                         ORDER BY tag LIMIT ?3",
                        )?;
                        collect_rows(statement.query_map(
                            params![scope_namespace, repository, limit],
                            entry_from_row,
                        )?)?
                    };
                verify_oci_tag_listing_evidence(&transaction, &values)?;
                transaction.commit()?;
                Ok(values)
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn list_oci_tags_by_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        digest_hex: &str,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let repository = repository.to_owned();
        let digest_hex = digest_hex.to_owned();
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let values = {
                    let mut statement = transaction.prepare(
                        "SELECT scope_namespace, repository, tag, digest_hex
                         FROM shardline_oci_tags
                         WHERE scope_namespace = ?1 AND repository = ?2 AND digest_hex = ?3
                         ORDER BY tag",
                    )?;
                    collect_rows(statement.query_map(
                        params![scope_namespace, repository, digest_hex],
                        entry_from_row,
                    )?)?
                };
                verify_oci_tag_listing_evidence(&transaction, &values)?;
                transaction.commit()?;
                Ok(values)
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn delete_oci_tag_if_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
        digest_hex: &str,
    ) -> Result<bool, Self::Error> {
        let store = self.clone();
        let scope_namespace = scope_namespace.to_owned();
        let repository = repository.to_owned();
        let tag = tag.to_owned();
        let digest_hex = digest_hex.to_owned();
        tokio::task::spawn_blocking(move || {
            super::helpers::retry_sqlite_busy(|| {
                let mut connection = store.open_connection()?;
                let transaction = connection.transaction()?;
                let changed = transaction.execute(
                    "DELETE FROM shardline_oci_tags
                     WHERE scope_namespace = ?1 AND repository = ?2 AND tag = ?3 AND digest_hex = ?4",
                    params![scope_namespace, repository, tag, digest_hex],
                )?;
                if changed == 1 {
                    record_tag_transition(
                        &transaction,
                        &scope_namespace,
                        &repository,
                        &tag,
                        Some(digest_hex.clone()),
                        None,
                    )?;
                }
                transaction.commit()?;
                Ok(changed == 1)
            })
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn entry(tag: &str, digest: &str) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-local-test".to_owned(),
            repository: "team/assets".to_owned(),
            tag: tag.to_owned(),
            digest_hex: digest.to_owned(),
        }
    }

    #[tokio::test]
    async fn local_oci_tags_list_and_digest_guarded_delete() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let original = entry("latest", &"a".repeat(64));
        let retargeted = entry("latest", &"b".repeat(64));
        let versioned = entry("v1", &"a".repeat(64));

        assert!(store.insert_oci_tag_if_absent(&original).await.unwrap());
        assert!(!store.insert_oci_tag_if_absent(&retargeted).await.unwrap());
        store.upsert_oci_tag(&versioned).await.unwrap();
        assert_eq!(
            store
                .list_oci_tags("oci-local-test", "team/assets", None, 10)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["latest", "v1"]
        );

        let observed = store
            .list_oci_tags_by_digest("oci-local-test", "team/assets", &"a".repeat(64))
            .await
            .unwrap();
        assert_eq!(observed.len(), 2);
        store.upsert_oci_tag(&retargeted).await.unwrap();
        assert!(
            !store
                .delete_oci_tag_if_digest(
                    "oci-local-test",
                    "team/assets",
                    "latest",
                    &"a".repeat(64),
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .oci_tag("oci-local-test", "team/assets", "latest")
                .await
                .unwrap(),
            Some(retargeted)
        );
    }

    #[tokio::test]
    async fn local_oci_tag_read_rejects_tampered_evidence() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("latest", &"a".repeat(64));
        store.upsert_oci_tag(&value).await.unwrap();

        let connection =
            rusqlite::Connection::open(storage.path().join("metadata.sqlite3")).unwrap();
        connection
            .execute(
                "UPDATE shardline_reliability_events
                 SET event_json = '{\"tampered\":true}'
                 WHERE operation_kind = 'OciTag'",
                [],
            )
            .unwrap();

        assert!(
            store
                .oci_tag(&value.scope_namespace, &value.repository, &value.tag)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_oci_tag_read_rejects_missing_baseline_evidence() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("latest", &"a".repeat(64));
        store.upsert_oci_tag(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'OciTag'",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(
            store
                .oci_tag(&value.scope_namespace, &value.repository, &value.tag)
                .await
                .is_err()
        );
        let repaired_connection = store.open_connection().unwrap();
        let count: i64 = repaired_connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_reliability_events
                 WHERE operation_kind = 'OciTag'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn local_oci_tag_list_rejects_missing_baseline_without_writing() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let value = entry("latest", &"a".repeat(64));
        store.upsert_oci_tag(&value).await.unwrap();

        let connection = store.open_connection().unwrap();
        connection
            .execute(
                "DELETE FROM shardline_reliability_events
                 WHERE operation_kind = 'OciTag'",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(
            store
                .list_oci_tags(&value.scope_namespace, &value.repository, None, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .list_oci_tags_by_digest(
                    &value.scope_namespace,
                    &value.repository,
                    &value.digest_hex,
                )
                .await
                .is_err()
        );
        let other_connection = store.open_connection().unwrap();
        let count: i64 = other_connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_reliability_events
                 WHERE operation_kind = 'OciTag'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_oci_tag_concurrent_retargets_keep_a_verifiable_pointer() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf()).unwrap();
        let first = entry("latest", &"a".repeat(64));
        let second = entry("latest", &"b".repeat(64));

        let (first_result, second_result) =
            tokio::join!(store.upsert_oci_tag(&first), store.upsert_oci_tag(&second),);
        first_result.unwrap();
        second_result.unwrap();

        let current = store
            .oci_tag(&first.scope_namespace, &first.repository, &first.tag)
            .await
            .unwrap();
        assert!(current == Some(first) || current == Some(second));
    }
}
