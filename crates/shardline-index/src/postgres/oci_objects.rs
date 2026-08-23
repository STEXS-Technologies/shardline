use sqlx::{Connection as _, PgConnection, Row as _, query, query_scalar};

use super::{PostgresIndexStore, PostgresMetadataStoreError};
use crate::{
    OciObjectKey, OciObjectKind, OciObjectStore, OciObjectTombstone, OciTagEntry,
    ResumableCompletionFence,
};

impl PostgresIndexStore {
    /// Publishes an OCI object through the session that owns its repository fence.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be committed.
    pub async fn publish_oci_object_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        for tag in tags {
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Publishes an OCI object and completes its resumable session in the same
    /// transaction, rejecting a superseded completion fence.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres cannot validate or commit the transaction.
    pub async fn publish_oci_object_completion_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
        fence: &ResumableCompletionFence,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let owns_completion = query_scalar::<_, i32>(
            "SELECT 1 FROM shardline_resumable_sessions
             WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2
               AND expires_at > clock_timestamp()
             FOR UPDATE",
        )
        .bind(fence.session_id())
        .bind(super::u64_to_i64(fence.epoch().get())?)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !owns_completion {
            transaction.rollback().await?;
            return Ok(false);
        }
        query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        for tag in tags {
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        let completed = query(
            "UPDATE shardline_resumable_sessions SET state = 'completed', updated_at = now()
             WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2",
        )
        .bind(fence.session_id())
        .bind(super::u64_to_i64(fence.epoch().get())?)
        .execute(&mut *transaction)
        .await?;
        if completed.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        transaction.commit().await?;
        Ok(true)
    }

    /// Logically deletes an OCI object through the lock-owning session.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be committed.
    pub async fn delete_oci_object_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        query(
            "INSERT INTO shardline_oci_object_tombstones
                (scope_namespace, repository, object_kind, digest_hex, deleted_at_unix_seconds)
             VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
             ON CONFLICT (scope_namespace, repository, object_kind, digest_hex)
             DO UPDATE SET deleted_at_unix_seconds = EXCLUDED.deleted_at_unix_seconds",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        if key.kind == OciObjectKind::Manifest {
            query(
                "DELETE FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3",
            )
            .bind(&key.scope_namespace)
            .bind(&key.repository)
            .bind(&key.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl OciObjectStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn oci_object_is_deleted(&self, key: &OciObjectKey) -> Result<bool, Self::Error> {
        Ok(query_scalar::<_, i32>(
            "SELECT 1 FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    async fn publish_oci_object(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), Self::Error> {
        let mut connection = self.pool.acquire().await?;
        self.publish_oci_object_on_connection(&mut connection, key, tags)
            .await
    }

    async fn delete_oci_object(&self, key: &OciObjectKey) -> Result<(), Self::Error> {
        let mut connection = self.pool.acquire().await?;
        self.delete_oci_object_on_connection(&mut connection, key)
            .await
    }

    async fn list_oci_object_tombstones(&self) -> Result<Vec<OciObjectTombstone>, Self::Error> {
        let rows = query(
            "SELECT scope_namespace, repository, object_kind, digest_hex,
                    deleted_at_unix_seconds
             FROM shardline_oci_object_tombstones
             ORDER BY scope_namespace, repository, object_kind, digest_hex",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let object_kind: String = row.try_get("object_kind")?;
                let kind = object_kind.parse()?;
                Ok(OciObjectTombstone {
                    key: OciObjectKey {
                        scope_namespace: row.try_get("scope_namespace")?,
                        repository: row.try_get("repository")?,
                        kind,
                        digest_hex: row.try_get("digest_hex")?,
                    },
                    deleted_at_unix_seconds: super::i64_to_u64(
                        row.try_get("deleted_at_unix_seconds")?,
                    )?,
                })
            })
            .collect()
    }

    async fn delete_oci_object_tombstone_if_unchanged(
        &self,
        tombstone: &OciObjectTombstone,
    ) -> Result<bool, Self::Error> {
        let result = query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4
               AND deleted_at_unix_seconds = $5",
        )
        .bind(&tombstone.key.scope_namespace)
        .bind(&tombstone.key.repository)
        .bind(tombstone.key.kind.as_str())
        .bind(&tombstone.key.digest_hex)
        .bind(super::u64_to_i64(tombstone.deleted_at_unix_seconds)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() != 0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::OciTagStore as _;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn object() -> OciObjectKey {
        OciObjectKey {
            scope_namespace: "oci-tombstone-pg".to_owned(),
            repository: "team/assets".to_owned(),
            kind: OciObjectKind::Manifest,
            digest_hex: "a".repeat(64),
        }
    }

    fn tag(name: &str, digest: char) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-tombstone-pg".to_owned(),
            repository: "team/assets".to_owned(),
            tag: name.to_owned(),
            digest_hex: digest.to_string().repeat(64),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_manifest_tombstone_and_tags_share_one_commit() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        query("DELETE FROM shardline_oci_object_tombstones WHERE scope_namespace = $1")
            .bind("oci-tombstone-pg")
            .execute(&pool)
            .await
            .unwrap();
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind("oci-tombstone-pg")
            .execute(&pool)
            .await
            .unwrap();
        let store = PostgresIndexStore::new(pool);
        let manifest = object();
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

        let tombstone = store
            .list_oci_object_tombstones()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.key == manifest)
            .unwrap();
        let mut stale = tombstone.clone();
        stale.deleted_at_unix_seconds = stale.deleted_at_unix_seconds.saturating_add(1);
        assert!(
            !store
                .delete_oci_object_tombstone_if_unchanged(&stale)
                .await
                .unwrap()
        );
        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
    }
}
