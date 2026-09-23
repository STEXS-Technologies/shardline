use rusqlite::{OptionalExtension, Transaction, params};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows};
use crate::{
    OciTagEntry, OciTagStore,
    local_sqlite::{current_oci_tag_evidence, oci_tag_snapshot, persist_oci_tag_evidence},
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
    let mut evidence =
        current_oci_tag_evidence(transaction, scope_namespace, repository, tag, before)?;
    evidence.record(oci_tag_snapshot(scope_namespace, repository, tag, after)?)?;
    for event in evidence.events() {
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

#[async_trait::async_trait]
impl OciTagStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn upsert_oci_tag(&self, entry: &OciTagEntry) -> Result<(), Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
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
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
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
                current_oci_tag_evidence(
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
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
            let value = current_tag(&transaction, &scope_namespace, &repository, &tag)?;
            current_oci_tag_evidence(
                &transaction,
                &scope_namespace,
                &repository,
                &tag,
                value.as_ref().map(|entry| entry.digest_hex.clone()),
            )?;
            transaction.commit()?;
            Ok(value)
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
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
            let limit = i64::try_from(limit)
                .map_err(|error| LocalIndexStoreError::IntegerOutOfRange(error.to_string()))?;
            let values = if let Some(cursor) = cursor {
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
                collect_rows(
                    statement
                        .query_map(params![scope_namespace, repository, limit], entry_from_row)?,
                )?
            };
            for value in &values {
                current_oci_tag_evidence(
                    &transaction,
                    &value.scope_namespace,
                    &value.repository,
                    &value.tag,
                    Some(value.digest_hex.clone()),
                )?;
            }
            transaction.commit()?;
            Ok(values)
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
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
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
            for value in &values {
                current_oci_tag_evidence(
                    &transaction,
                    &value.scope_namespace,
                    &value.repository,
                    &value.tag,
                    Some(value.digest_hex.clone()),
                )?;
            }
            transaction.commit()?;
            Ok(values)
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
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
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
                    Some(digest_hex),
                    None,
                )?;
            }
            transaction.commit()?;
            Ok(changed == 1)
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
}
