use sqlx::{Row, postgres::PgRow, query};

use super::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{RepoKey, RevisionRecord, TreeEntry, TreeEntryOutcome, TreeKey, TreeStore};

fn tree_entry_from_row(row: &PgRow) -> Result<TreeEntry, PostgresMetadataStoreError> {
    Ok(TreeEntry {
        provider: row.try_get("provider")?,
        owner: row.try_get("owner")?,
        repo: row.try_get("repo")?,
        revision: row.try_get("revision")?,
        path: row.try_get("path")?,
        file_id: row.try_get("file_id")?,
        size_bytes: i64_to_u64(row.try_get("size_bytes")?)?,
        updated_at_unix_seconds: i64_to_u64(row.try_get("updated_at_unix_seconds")?)?,
    })
}

fn revision_record_from_row(row: &PgRow) -> Result<RevisionRecord, PostgresMetadataStoreError> {
    Ok(RevisionRecord {
        provider: row.try_get("provider")?,
        owner: row.try_get("owner")?,
        repo: row.try_get("repo")?,
        revision: row.try_get("revision")?,
        created_at_unix_seconds: i64_to_u64(row.try_get("created_at_unix_seconds")?)?,
        updated_at_unix_seconds: i64_to_u64(row.try_get("updated_at_unix_seconds")?)?,
    })
}

#[async_trait::async_trait]
impl TreeStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_tree_entry(&self, entry: &TreeEntry) -> Result<TreeEntryOutcome, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let existed: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM shardline_tree_entries
                WHERE provider = $1 AND owner = $2 AND repo = $3
                  AND revision = $4 AND path = $5
             )",
        )
        .bind(&entry.provider)
        .bind(&entry.owner)
        .bind(&entry.repo)
        .bind(&entry.revision)
        .bind(&entry.path)
        .fetch_one(&mut *transaction)
        .await?;
        query(
            "INSERT INTO shardline_tree_entries (
                provider, owner, repo, revision, path, file_id,
                size_bytes, updated_at_unix_seconds
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (provider, owner, repo, revision, path)
             DO UPDATE SET
                file_id = EXCLUDED.file_id,
                size_bytes = EXCLUDED.size_bytes,
                updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds",
        )
        .bind(&entry.provider)
        .bind(&entry.owner)
        .bind(&entry.repo)
        .bind(&entry.revision)
        .bind(&entry.path)
        .bind(&entry.file_id)
        .bind(u64_to_i64(entry.size_bytes)?)
        .bind(u64_to_i64(entry.updated_at_unix_seconds)?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(TreeEntryOutcome { created: !existed })
    }

    async fn tree_entry(
        &self,
        key: &TreeKey,
        path: &str,
    ) -> Result<Option<TreeEntry>, Self::Error> {
        let row = query(
            "SELECT provider, owner, repo, revision, path, file_id,
                    size_bytes, updated_at_unix_seconds
             FROM shardline_tree_entries
             WHERE provider = $1 AND owner = $2 AND repo = $3
               AND revision = $4 AND path = $5",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(&key.revision)
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(tree_entry_from_row).transpose()
    }

    async fn delete_tree_entries(
        &self,
        key: &TreeKey,
        path: &str,
        recursive: bool,
    ) -> Result<u64, Self::Error> {
        let result = if recursive {
            query(
                "DELETE FROM shardline_tree_entries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4
                   AND (path = $5 OR substr(path, 1, length($5) + 1) = $5 || '/')",
            )
            .bind(&key.provider)
            .bind(&key.owner)
            .bind(&key.repo)
            .bind(&key.revision)
            .bind(path)
            .execute(&self.pool)
            .await?
        } else {
            query(
                "DELETE FROM shardline_tree_entries
                 WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4 AND path = $5",
            )
            .bind(&key.provider)
            .bind(&key.owner)
            .bind(&key.repo)
            .bind(&key.revision)
            .bind(path)
            .execute(&self.pool)
            .await?
        };
        Ok(result.rows_affected())
    }

    async fn scan_tree(
        &self,
        key: &TreeKey,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TreeEntry>, Self::Error> {
        use std::fmt::Write as _;

        let mut sql = String::from(
            "SELECT provider, owner, repo, revision, path, file_id,
                    size_bytes, updated_at_unix_seconds
             FROM shardline_tree_entries
             WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4",
        );
        let mut index = 5usize;
        if !prefix.is_empty() {
            write!(
                sql,
                " AND (path = ${index} OR substr(path, 1, length(${index}) + 1) = ${index} || '/')"
            )
            .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
            index = index.saturating_add(1);
        }
        if cursor.is_some() {
            write!(sql, " AND path > ${index}")
                .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
            index = index.saturating_add(1);
        }
        let limit_i64 = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        sql.push_str(" ORDER BY path");
        write!(sql, " LIMIT ${index}")
            .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;

        let mut q = query(&sql)
            .bind(&key.provider)
            .bind(&key.owner)
            .bind(&key.repo)
            .bind(&key.revision);
        if !prefix.is_empty() {
            q = q.bind(prefix);
        }
        if let Some(cursor) = cursor {
            q = q.bind(cursor);
        }
        q = q.bind(limit_i64);
        let rows = q.fetch_all(&self.pool).await?;
        rows.iter().map(tree_entry_from_row).collect()
    }

    async fn upsert_revision(&self, rev: &RevisionRecord) -> Result<bool, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let existed: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM shardline_revisions
                WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4
             )",
        )
        .bind(&rev.provider)
        .bind(&rev.owner)
        .bind(&rev.repo)
        .bind(&rev.revision)
        .fetch_one(&mut *transaction)
        .await?;
        query(
            "INSERT INTO shardline_revisions (
                provider, owner, repo, revision, created_at_unix_seconds, updated_at_unix_seconds
             )
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (provider, owner, repo, revision)
             DO UPDATE SET
                created_at_unix_seconds = EXCLUDED.created_at_unix_seconds,
                updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds",
        )
        .bind(&rev.provider)
        .bind(&rev.owner)
        .bind(&rev.repo)
        .bind(&rev.revision)
        .bind(u64_to_i64(rev.created_at_unix_seconds)?)
        .bind(u64_to_i64(rev.updated_at_unix_seconds)?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(!existed)
    }

    async fn revision(
        &self,
        key: &RepoKey,
        rev: &str,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        let row = query(
            "SELECT provider, owner, repo, revision, created_at_unix_seconds,
                    updated_at_unix_seconds
             FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(rev)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(revision_record_from_row).transpose()
    }

    async fn list_revisions(&self, key: &RepoKey) -> Result<Vec<RevisionRecord>, Self::Error> {
        let rows = query(
            "SELECT provider, owner, repo, revision, created_at_unix_seconds,
                    updated_at_unix_seconds
             FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3
             ORDER BY revision",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(revision_record_from_row).collect()
    }

    async fn delete_revision(&self, key: &RepoKey, rev: &str) -> Result<u64, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        query(
            "DELETE FROM shardline_tree_entries
             WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(rev)
        .execute(&mut *transaction)
        .await?;
        let result = query(
            "DELETE FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3 AND revision = $4",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(rev)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected())
    }
}
