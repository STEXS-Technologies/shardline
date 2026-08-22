use rusqlite::{OptionalExtension, params};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows};
use crate::{OciTagEntry, OciTagStore};

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
            store.open_connection()?.execute(
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
            Ok(())
        })
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
    }

    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let changed = store.open_connection()?.execute(
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
            store
                .open_connection()?
                .query_row(
                    "SELECT scope_namespace, repository, tag, digest_hex
                     FROM shardline_oci_tags
                     WHERE scope_namespace = ?1 AND repository = ?2 AND tag = ?3",
                    params![scope_namespace, repository, tag],
                    entry_from_row,
                )
                .optional()
                .map_err(Into::into)
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
            let limit = i64::try_from(limit)
                .map_err(|error| LocalIndexStoreError::IntegerOutOfRange(error.to_string()))?;
            if let Some(cursor) = cursor {
                let mut statement = connection.prepare(
                    "SELECT scope_namespace, repository, tag, digest_hex
                     FROM shardline_oci_tags
                     WHERE scope_namespace = ?1 AND repository = ?2 AND tag > ?3
                     ORDER BY tag LIMIT ?4",
                )?;
                collect_rows(statement.query_map(
                    params![scope_namespace, repository, cursor, limit],
                    entry_from_row,
                )?)
            } else {
                let mut statement = connection.prepare(
                    "SELECT scope_namespace, repository, tag, digest_hex
                     FROM shardline_oci_tags
                     WHERE scope_namespace = ?1 AND repository = ?2
                     ORDER BY tag LIMIT ?3",
                )?;
                collect_rows(
                    statement
                        .query_map(params![scope_namespace, repository, limit], entry_from_row)?,
                )
            }
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
            let mut statement = connection.prepare(
                "SELECT scope_namespace, repository, tag, digest_hex
                 FROM shardline_oci_tags
                 WHERE scope_namespace = ?1 AND repository = ?2 AND digest_hex = ?3
                 ORDER BY tag",
            )?;
            collect_rows(statement.query_map(
                params![scope_namespace, repository, digest_hex],
                entry_from_row,
            )?)
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
            let changed = store.open_connection()?.execute(
                "DELETE FROM shardline_oci_tags
                 WHERE scope_namespace = ?1 AND repository = ?2 AND tag = ?3 AND digest_hex = ?4",
                params![scope_namespace, repository, tag, digest_hex],
            )?;
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
}
