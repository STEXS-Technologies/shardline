use rusqlite::{OptionalExtension, params};

use super::{LocalIndexStore, LocalIndexStoreError, i64_to_u64};
use crate::{OciObjectKey, OciObjectKind, OciObjectStore, OciObjectTombstone, OciTagEntry};

impl LocalIndexStore {
    fn list_oci_object_tombstones_blocking(
        &self,
    ) -> Result<Vec<OciObjectTombstone>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
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
        Ok(tombstones)
    }

    fn publish_oci_object_blocking(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
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
        transaction.commit()?;
        Ok(())
    }

    fn delete_oci_object_blocking(&self, key: &OciObjectKey) -> Result<(), LocalIndexStoreError> {
        let mut connection = self.open_connection()?;
        let transaction = connection.transaction()?;
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
            let found = store
                .open_connection()?
                .query_row(
                    "SELECT 1 FROM shardline_oci_object_tombstones
                     WHERE scope_namespace = ?1 AND repository = ?2
                       AND object_kind = ?3 AND digest_hex = ?4",
                    params![
                        key.scope_namespace,
                        key.repository,
                        key.kind.as_str(),
                        key.digest_hex
                    ],
                    |_row| Ok(()),
                )
                .optional()?;
            Ok(found.is_some())
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
            let deleted = store.open_connection()?.execute(
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
}
