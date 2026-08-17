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

    async fn count_revisions(&self, key: &RepoKey) -> Result<u64, Self::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .fetch_one(&self.pool)
        .await?;
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn count_tree_entries(&self, key: &RepoKey) -> Result<u64, Self::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM shardline_tree_entries
             WHERE provider = $1 AND owner = $2 AND repo = $3",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .fetch_one(&self.pool)
        .await?;
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn list_revisions(
        &self,
        key: &RepoKey,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, Self::Error> {
        use std::fmt::Write as _;

        let mut sql = String::from(
            "SELECT provider, owner, repo, revision, created_at_unix_seconds,
                    updated_at_unix_seconds
             FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3",
        );
        let mut index = 4usize;
        if cursor.is_some() {
            write!(sql, " AND revision > ${index}")
                .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;
            index = index.saturating_add(1);
        }
        let limit_i64 = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        sql.push_str(" ORDER BY revision");
        write!(sql, " LIMIT ${index}")
            .map_err(|e| PostgresMetadataStoreError::IntegerOutOfRange(e.to_string()))?;

        let mut q = query(&sql)
            .bind(&key.provider)
            .bind(&key.owner)
            .bind(&key.repo);
        if let Some(cursor) = cursor {
            q = q.bind(cursor);
        }
        q = q.bind(limit_i64);
        let rows = q.fetch_all(&self.pool).await?;
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

    async fn prune_revisions_over_cap(
        &self,
        key: &RepoKey,
        max_revisions: usize,
    ) -> Result<u64, Self::Error> {
        let count = self.count_revisions(key).await?;
        let cap = u64::try_from(max_revisions).unwrap_or(u64::MAX);
        let Some(prune_limit) = count.checked_sub(cap) else {
            return Ok(0);
        };
        if prune_limit == 0 {
            return Ok(0);
        }
        let limit_i64 = u64_to_i64(prune_limit)?;
        // Both subqueries select the same oldest rows (oldest-created first,
        // revision name as the deterministic tiebreaker): the tree-entry
        // delete does not touch `shardline_revisions`, so the second subquery
        // still sees the full pre-prune row set.
        let mut transaction = self.pool.begin().await?;
        query(
            "DELETE FROM shardline_tree_entries
             WHERE provider = $1 AND owner = $2 AND repo = $3
               AND revision IN (
                   SELECT revision FROM shardline_revisions
                   WHERE provider = $1 AND owner = $2 AND repo = $3
                   ORDER BY created_at_unix_seconds, revision
                   LIMIT $4
               )",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(limit_i64)
        .execute(&mut *transaction)
        .await?;
        let result = query(
            "DELETE FROM shardline_revisions
             WHERE provider = $1 AND owner = $2 AND repo = $3
               AND revision IN (
                   SELECT revision FROM shardline_revisions
                   WHERE provider = $1 AND owner = $2 AND repo = $3
                   ORDER BY created_at_unix_seconds, revision
                   LIMIT $4
               )",
        )
        .bind(&key.provider)
        .bind(&key.owner)
        .bind(&key.repo)
        .bind(limit_i64)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected())
    }

    async fn list_revision_repo_keys(&self) -> Result<Vec<RepoKey>, Self::Error> {
        let rows = query(
            "SELECT DISTINCT provider, owner, repo
             FROM shardline_revisions
             ORDER BY provider, owner, repo",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(RepoKey {
                    provider: row.try_get("provider")?,
                    owner: row.try_get("owner")?,
                    repo: row.try_get("repo")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use super::*;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn tree_key(revision: &str) -> TreeKey {
        TreeKey::new("github", "owner", "repo", revision)
    }

    fn repo_key() -> RepoKey {
        RepoKey::new("github", "owner", "repo")
    }

    fn entry(revision: &str, path: &str) -> TreeEntry {
        TreeEntry {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: revision.to_owned(),
            path: path.to_owned(),
            file_id: "ab".repeat(32),
            size_bytes: 123,
            updated_at_unix_seconds: 1000,
        }
    }

    /// Round-trips the Postgres TreeStore against a migrated schema (created by
    /// `shardline -- db migrate up`, which the `postgres` CI job runs first).
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_tree_store_upsert_scan_delete_roundtrip() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let key = tree_key("main");

        let e = entry("main", "data/model.pt");
        assert!(
            TreeStore::upsert_tree_entry(&store, &e)
                .await
                .expect("upsert")
                .created
        );
        assert!(
            !TreeStore::upsert_tree_entry(&store, &e)
                .await
                .expect("upsert")
                .created
        );

        let loaded = TreeStore::tree_entry(&store, &key, "data/model.pt")
            .await
            .expect("tree_entry")
            .expect("present");
        assert_eq!(loaded.file_id, e.file_id);
        assert_eq!(loaded.size_bytes, 123);

        let scanned = TreeStore::scan_tree(&store, &key, "data", None, 100)
            .await
            .expect("scan");
        assert_eq!(scanned.len(), 1);
        let cursor = scanned[0].path.clone();
        assert!(
            TreeStore::scan_tree(&store, &key, "", Some(&cursor), 100)
                .await
                .expect("scan cursor")
                .is_empty()
        );

        let removed = TreeStore::delete_tree_entries(&store, &key, "data", true)
            .await
            .expect("delete");
        assert_eq!(removed, 1);
        assert!(
            TreeStore::tree_entry(&store, &key, "data/model.pt")
                .await
                .expect("lookup")
                .is_none()
        );
        // Clean up any revision registry rows created by other tests in this run.
        let _ = TreeStore::delete_revision(&store, &repo_key(), "main")
            .await
            .expect("delete revision");
    }

    /// Exercises the revision registry (upsert / read / list / delete cascade).
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_revision_registry_lifecycle() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let rev = RevisionRecord {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: "feature".to_owned(),
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        assert!(
            TreeStore::upsert_revision(&store, &rev)
                .await
                .expect("upsert")
        );
        assert!(
            !TreeStore::upsert_revision(&store, &rev)
                .await
                .expect("upsert")
        );

        let loaded = TreeStore::revision(&store, &repo_key(), "feature")
            .await
            .expect("revision")
            .expect("present");
        assert_eq!(loaded.revision, "feature");

        let listed = TreeStore::list_revisions(&store, &repo_key(), None, 100)
            .await
            .expect("list");
        assert_eq!(listed.len(), 1);

        TreeStore::upsert_tree_entry(&store, &entry("feature", "x.txt"))
            .await
            .expect("upsert");
        let removed = TreeStore::delete_revision(&store, &repo_key(), "feature")
            .await
            .expect("delete revision");
        assert_eq!(removed, 1);
        assert!(
            TreeStore::revision(&store, &repo_key(), "feature")
                .await
                .expect("revision")
                .is_none()
        );
    }

    /// Exercises the F-75 GC-side prune: oldest-created rows beyond the cap
    /// are evicted (created-at ordering, not name ordering), tree rows
    /// cascade, and other repos are untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_prune_revisions_over_cap_removes_oldest_down_to_cap() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let cap = 2usize;
        // created_at deliberately NOT aligned with name order.
        for (n, created_at) in [("rev-a", 100u64), ("rev-b", 200), ("rev-c", 300)] {
            let rev = RevisionRecord {
                provider: "github".to_owned(),
                owner: "owner".to_owned(),
                repo: "repo".to_owned(),
                revision: n.to_owned(),
                created_at_unix_seconds: created_at,
                updated_at_unix_seconds: created_at,
            };
            assert!(
                TreeStore::upsert_revision(&store, &rev)
                    .await
                    .expect("upsert")
            );
        }
        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), cap)
            .await
            .expect("prune");
        assert_eq!(removed, 1);
        let remaining = TreeStore::list_revisions(&store, &repo_key(), None, 100)
            .await
            .expect("list");
        let names: Vec<&str> = remaining.iter().map(|r| r.revision.as_str()).collect();
        // Only the two newest-created rows survive regardless of name order.
        assert_eq!(names, vec!["rev-b", "rev-c"]);
        // Clean up so the shared test database stays tidy for other runs.
        for name in names {
            let _ = TreeStore::delete_revision(&store, &repo_key(), name)
                .await
                .expect("cleanup");
        }
    }

    /// The prune is a no-op at/below the cap and does not touch other repos.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_prune_revisions_over_cap_at_cap_and_other_repo_untouched() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let other = RepoKey::new("github", "owner", "other-repo");
        let cap = 5usize;
        for n in 0..3u8 {
            let rev = RevisionRecord {
                provider: "github".to_owned(),
                owner: "owner".to_owned(),
                repo: "repo".to_owned(),
                revision: format!("rev-{n:02}"),
                created_at_unix_seconds: u64::from(n),
                updated_at_unix_seconds: u64::from(n),
            };
            assert!(
                TreeStore::upsert_revision(&store, &rev)
                    .await
                    .expect("upsert")
            );
        }
        let other_rev = RevisionRecord {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "other-repo".to_owned(),
            revision: "main".to_owned(),
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        assert!(
            TreeStore::upsert_revision(&store, &other_rev)
                .await
                .expect("upsert")
        );
        // Below the cap: nothing to prune.
        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), cap)
            .await
            .expect("prune");
        assert_eq!(removed, 0);
        // The other repo is untouched.
        assert_eq!(TreeStore::count_revisions(&store, &other).await.unwrap(), 1);
        // Clean up.
        for n in 0..3u8 {
            let _ = TreeStore::delete_revision(&store, &repo_key(), &format!("rev-{n:02}"))
                .await
                .expect("cleanup");
        }
        let _ = TreeStore::delete_revision(&store, &other, "main")
            .await
            .expect("cleanup other");
    }

    /// Lists the distinct repos present in the revision registry.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_list_revision_repo_keys_returns_distinct_repos() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let repo = repo_key();
        let other = RepoKey::new("github", "owner", "other-repo");
        for key in [&repo, &repo, &other] {
            let rev = RevisionRecord {
                provider: key.provider.clone(),
                owner: key.owner.clone(),
                repo: key.repo.clone(),
                revision: "main".to_owned(),
                created_at_unix_seconds: 1,
                updated_at_unix_seconds: 1,
            };
            assert!(
                TreeStore::upsert_revision(&store, &rev)
                    .await
                    .expect("upsert")
            );
        }
        let keys = TreeStore::list_revision_repo_keys(&store)
            .await
            .expect("list repo keys");
        assert!(keys.contains(&repo));
        assert!(keys.contains(&other));
        // Clean up.
        let _ = TreeStore::delete_revision(&store, &repo, "main")
            .await
            .expect("cleanup");
        let _ = TreeStore::delete_revision(&store, &other, "main")
            .await
            .expect("cleanup other");
    }

    /// Counts tree-entry rows per repo (F-103 cap gate) across revisions.
    #[tokio::test(flavor = "multi_thread")]
    async fn pg_count_tree_entries_counts_only_the_matching_repo() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool);
        let repo = repo_key();
        let other = RepoKey::new("github", "owner", "other-repo");
        assert_eq!(
            TreeStore::count_tree_entries(&store, &repo)
                .await
                .expect("count"),
            0
        );
        // Distinct paths across multiple revisions all count against the repo.
        for (revision, path) in [("main", "a.txt"), ("main", "b.txt"), ("feature", "c.txt")] {
            assert!(
                TreeStore::upsert_tree_entry(&store, &entry(revision, path))
                    .await
                    .expect("upsert")
                    .created
            );
        }
        assert_eq!(
            TreeStore::count_tree_entries(&store, &repo)
                .await
                .expect("count"),
            3
        );
        // A same-path upsert does not grow the count; a different repo is not
        // counted against this repository.
        assert!(
            !TreeStore::upsert_tree_entry(&store, &entry("main", "a.txt"))
                .await
                .expect("upsert")
                .created
        );
        assert_eq!(
            TreeStore::count_tree_entries(&store, &repo)
                .await
                .expect("count"),
            3
        );
        assert_eq!(
            TreeStore::count_tree_entries(&store, &other)
                .await
                .expect("count other"),
            0
        );
        // Clean up so the shared test database stays tidy for other runs.
        for revision in ["main", "feature"] {
            let _ = TreeStore::delete_revision(&store, &repo, revision)
                .await
                .expect("cleanup");
        }
    }
}
