use rusqlite::{OptionalExtension, params};

use super::{LocalIndexStore, LocalIndexStoreError};
use crate::{OciObjectKey, OciObjectKind, OciObjectStore, OciTagEntry};

impl LocalIndexStore {
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
}
