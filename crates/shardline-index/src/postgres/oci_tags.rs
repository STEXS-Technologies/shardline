use sqlx::{PgConnection, Row, postgres::PgRow, query};

use super::{PostgresIndexStore, PostgresMetadataStoreError, u64_to_i64};
use crate::{OciTagEntry, OciTagStore};

fn entry_from_row(row: &PgRow) -> Result<OciTagEntry, PostgresMetadataStoreError> {
    Ok(OciTagEntry {
        scope_namespace: row.try_get("scope_namespace")?,
        repository: row.try_get("repository")?,
        tag: row.try_get("tag")?,
        digest_hex: row.try_get("digest_hex")?,
    })
}

impl PostgresIndexStore {
    /// Upserts an OCI tag through a caller-owned Postgres connection.
    ///
    /// This is used by the server's fenced resource guard so the mutation and
    /// session advisory lock share one database session.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when Postgres rejects the mutation.
    pub async fn upsert_oci_tag_on_connection(
        &self,
        connection: &mut PgConnection,
        entry: &OciTagEntry,
    ) -> Result<(), PostgresMetadataStoreError> {
        query(
            "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (scope_namespace, repository, tag)
             DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.repository)
        .bind(&entry.tag)
        .bind(&entry.digest_hex)
        .execute(connection)
        .await?;
        Ok(())
    }

    /// Digest-guarded OCI tag deletion through a caller-owned connection.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when Postgres rejects the mutation.
    pub async fn delete_oci_tag_if_digest_on_connection(
        &self,
        connection: &mut PgConnection,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
        digest_hex: &str,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let result = query(
            "DELETE FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3 AND digest_hex = $4",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .bind(digest_hex)
        .execute(connection)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}

#[async_trait::async_trait]
impl OciTagStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_oci_tag(&self, entry: &OciTagEntry) -> Result<(), Self::Error> {
        query(
            "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (scope_namespace, repository, tag)
             DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.repository)
        .bind(&entry.tag)
        .bind(&entry.digest_hex)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error> {
        let result = query(
            "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (scope_namespace, repository, tag) DO NOTHING",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.repository)
        .bind(&entry.tag)
        .bind(&entry.digest_hex)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, Self::Error> {
        query(
            "SELECT scope_namespace, repository, tag, digest_hex
             FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .fetch_optional(&self.pool)
        .await?
        .as_ref()
        .map(entry_from_row)
        .transpose()
    }

    async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        let limit = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        let rows = if let Some(cursor) = cursor {
            query(
                "SELECT scope_namespace, repository, tag, digest_hex
                 FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2 AND tag > $3
                 ORDER BY tag LIMIT $4",
            )
            .bind(scope_namespace)
            .bind(repository)
            .bind(cursor)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            query(
                "SELECT scope_namespace, repository, tag, digest_hex
                 FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2
                 ORDER BY tag LIMIT $3",
            )
            .bind(scope_namespace)
            .bind(repository)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        rows.iter().map(entry_from_row).collect()
    }

    async fn list_oci_tags_by_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        digest_hex: &str,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        query(
            "SELECT scope_namespace, repository, tag, digest_hex
             FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3
             ORDER BY tag",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(digest_hex)
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(entry_from_row)
        .collect()
    }

    async fn delete_oci_tag_if_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
        digest_hex: &str,
    ) -> Result<bool, Self::Error> {
        let result = query(
            "DELETE FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3 AND digest_hex = $4",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .bind(digest_hex)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn entry(digest: &str) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-pg-cas".to_owned(),
            repository: "team/assets".to_owned(),
            tag: "latest".to_owned(),
            digest_hex: digest.to_owned(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_tag_delete_cannot_remove_concurrent_retarget() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind("oci-pg-cas")
            .execute(&pool)
            .await
            .expect("clean OCI tag fixture");
        let deleting_store = PostgresIndexStore::new(pool.clone());
        let retargeting_store = PostgresIndexStore::new(pool);
        let old = entry(&"a".repeat(64));
        let new = entry(&"b".repeat(64));
        deleting_store.upsert_oci_tag(&old).await.unwrap();

        let observed = deleting_store
            .list_oci_tags_by_digest("oci-pg-cas", "team/assets", &old.digest_hex)
            .await
            .unwrap();
        assert_eq!(observed, vec![old.clone()]);
        retargeting_store.upsert_oci_tag(&new).await.unwrap();
        assert!(
            !deleting_store
                .delete_oci_tag_if_digest("oci-pg-cas", "team/assets", "latest", &old.digest_hex,)
                .await
                .unwrap()
        );
        assert_eq!(
            deleting_store
                .oci_tag("oci-pg-cas", "team/assets", "latest")
                .await
                .unwrap(),
            Some(new)
        );
    }
}
