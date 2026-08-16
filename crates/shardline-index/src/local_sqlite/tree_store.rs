use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use super::{LocalIndexStore, LocalIndexStoreError, collect_rows, helpers};
use crate::{RepoKey, RevisionRecord, TreeEntry, TreeEntryOutcome, TreeKey, TreeStore};

fn tree_entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TreeEntry> {
    Ok(TreeEntry {
        provider: row.get("provider")?,
        owner: row.get("owner")?,
        repo: row.get("repo")?,
        revision: row.get("revision")?,
        path: row.get("path")?,
        file_id: row.get("file_id")?,
        size_bytes: helpers::i64_to_u64(row.get("size_bytes")?).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?,
        updated_at_unix_seconds: helpers::i64_to_u64(row.get("updated_at_unix_seconds")?).map_err(
            |e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::new(e),
                )
            },
        )?,
    })
}

fn revision_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RevisionRecord> {
    Ok(RevisionRecord {
        provider: row.get("provider")?,
        owner: row.get("owner")?,
        repo: row.get("repo")?,
        revision: row.get("revision")?,
        created_at_unix_seconds: helpers::i64_to_u64(row.get("created_at_unix_seconds")?).map_err(
            |e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::new(e),
                )
            },
        )?,
        updated_at_unix_seconds: helpers::i64_to_u64(row.get("updated_at_unix_seconds")?).map_err(
            |e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::new(e),
                )
            },
        )?,
    })
}

fn upsert_tree_entry_sql(
    connection: &Connection,
    entry: &TreeEntry,
) -> Result<TreeEntryOutcome, LocalIndexStoreError> {
    let existed = connection.query_row(
        "SELECT EXISTS(
                SELECT 1 FROM shardline_tree_entries
                WHERE provider = ?1 AND owner = ?2 AND repo = ?3
                  AND revision = ?4 AND path = ?5
             )",
        params![
            entry.provider,
            entry.owner,
            entry.repo,
            entry.revision,
            entry.path
        ],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let size_bytes = helpers::u64_to_i64(entry.size_bytes)?;
    let updated_at = helpers::u64_to_i64(entry.updated_at_unix_seconds)?;
    connection.execute(
        "INSERT INTO shardline_tree_entries (
            provider, owner, repo, revision, path, file_id,
            size_bytes, updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (provider, owner, repo, revision, path)
         DO UPDATE SET
            file_id = excluded.file_id,
            size_bytes = excluded.size_bytes,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        params![
            entry.provider,
            entry.owner,
            entry.repo,
            entry.revision,
            entry.path,
            entry.file_id,
            size_bytes,
            updated_at,
        ],
    )?;
    Ok(TreeEntryOutcome { created: !existed })
}

fn tree_entry_sql(
    connection: &Connection,
    key: &TreeKey,
    path: &str,
) -> Result<Option<TreeEntry>, LocalIndexStoreError> {
    connection
        .query_row(
            "SELECT provider, owner, repo, revision, path, file_id,
                    size_bytes, updated_at_unix_seconds
             FROM shardline_tree_entries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3
               AND revision = ?4 AND path = ?5",
            params![key.provider, key.owner, key.repo, key.revision, path],
            tree_entry_from_row,
        )
        .optional()
        .map_err(LocalIndexStoreError::from)
}

fn delete_tree_entries_sql(
    connection: &Connection,
    key: &TreeKey,
    path: &str,
    recursive: bool,
) -> Result<u64, LocalIndexStoreError> {
    if recursive {
        let mut args: Vec<&dyn rusqlite::ToSql> =
            vec![&key.provider, &key.owner, &key.repo, &key.revision];
        let sql = "DELETE FROM shardline_tree_entries
                   WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4
                     AND (path = ?5 OR substr(path, 1, length(?5) + 1) = ?5 || '/')";
        let path_str = path.to_owned();
        args.push(&path_str);
        let changed = connection.execute(sql, params_from_iter(args))?;
        Ok(u64::try_from(changed).unwrap_or(u64::MAX))
    } else {
        let changed = connection.execute(
            "DELETE FROM shardline_tree_entries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4 AND path = ?5",
            params![key.provider, key.owner, key.repo, key.revision, path],
        )?;
        Ok(u64::try_from(changed).unwrap_or(u64::MAX))
    }
}

fn scan_tree_sql(
    connection: &Connection,
    key: &TreeKey,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<TreeEntry>, LocalIndexStoreError> {
    use rusqlite::types::Value;
    use std::fmt::Write as _;

    let mut sql = String::from(
        "SELECT provider, owner, repo, revision, path, file_id,
                size_bytes, updated_at_unix_seconds
         FROM shardline_tree_entries
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4",
    );
    let mut args: Vec<Value> = vec![
        Value::Text(key.provider.clone()),
        Value::Text(key.owner.clone()),
        Value::Text(key.repo.clone()),
        Value::Text(key.revision.clone()),
    ];
    let mut index = 5usize;
    if !prefix.is_empty() {
        write!(
            sql,
            " AND (path = ?{index} OR substr(path, 1, length(?{index}) + 1) = ?{index} || '/')"
        )
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
        args.push(Value::Text(prefix.to_owned()));
        index = index.saturating_add(1);
    }
    if let Some(cursor) = cursor {
        write!(sql, " AND path > ?{index}")
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
        args.push(Value::Text(cursor.to_owned()));
        index = index.saturating_add(1);
    }
    let limit_i64 =
        i64::try_from(limit).map_err(|e| LocalIndexStoreError::IntegerOutOfRange(e.to_string()))?;
    sql.push_str(" ORDER BY path");
    write!(sql, " LIMIT ?{index}")
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
    args.push(Value::Integer(limit_i64));

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(args.iter()), tree_entry_from_row)?;
    collect_rows(rows)
}

fn upsert_revision_sql(
    connection: &Connection,
    rev: &RevisionRecord,
) -> Result<bool, LocalIndexStoreError> {
    let existed = connection.query_row(
        "SELECT EXISTS(
                SELECT 1 FROM shardline_revisions
                WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4
             )",
        params![rev.provider, rev.owner, rev.repo, rev.revision],
        |row| row.get::<_, i64>(0),
    )? != 0;
    let created_at = helpers::u64_to_i64(rev.created_at_unix_seconds)?;
    let updated_at = helpers::u64_to_i64(rev.updated_at_unix_seconds)?;
    connection.execute(
        "INSERT INTO shardline_revisions (
            provider, owner, repo, revision, created_at_unix_seconds, updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (provider, owner, repo, revision)
         DO UPDATE SET
            created_at_unix_seconds = excluded.created_at_unix_seconds,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        params![
            rev.provider,
            rev.owner,
            rev.repo,
            rev.revision,
            created_at,
            updated_at,
        ],
    )?;
    Ok(!existed)
}

fn revision_sql(
    connection: &Connection,
    key: &RepoKey,
    rev: &str,
) -> Result<Option<RevisionRecord>, LocalIndexStoreError> {
    connection
        .query_row(
            "SELECT provider, owner, repo, revision, created_at_unix_seconds,
                    updated_at_unix_seconds
             FROM shardline_revisions
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4",
            params![key.provider, key.owner, key.repo, rev],
            revision_record_from_row,
        )
        .optional()
        .map_err(LocalIndexStoreError::from)
}

fn count_revisions_sql(
    connection: &Connection,
    key: &RepoKey,
) -> Result<u64, LocalIndexStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM shardline_revisions
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
        params![key.provider, key.owner, key.repo],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(u64::MAX))
}

fn list_revisions_sql(
    connection: &Connection,
    key: &RepoKey,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<RevisionRecord>, LocalIndexStoreError> {
    use rusqlite::types::Value;
    use std::fmt::Write as _;

    let mut sql = String::from(
        "SELECT provider, owner, repo, revision, created_at_unix_seconds,
                updated_at_unix_seconds
         FROM shardline_revisions
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
    );
    let mut args: Vec<Value> = vec![
        Value::Text(key.provider.clone()),
        Value::Text(key.owner.clone()),
        Value::Text(key.repo.clone()),
    ];
    let mut index = 4usize;
    if let Some(cursor) = cursor {
        write!(sql, " AND revision > ?{index}")
            .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
        args.push(Value::Text(cursor.to_owned()));
        index = index.saturating_add(1);
    }
    let limit_i64 =
        i64::try_from(limit).map_err(|e| LocalIndexStoreError::IntegerOutOfRange(e.to_string()))?;
    sql.push_str(" ORDER BY revision");
    write!(sql, " LIMIT ?{index}")
        .map_err(|e| LocalIndexStoreError::Io(std::io::Error::other(e)))?;
    args.push(Value::Integer(limit_i64));

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(args.iter()), revision_record_from_row)?;
    collect_rows(rows)
}

fn delete_revision_sql(
    connection: &Connection,
    key: &RepoKey,
    rev: &str,
) -> Result<u64, LocalIndexStoreError> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute(
        "DELETE FROM shardline_tree_entries
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4",
        params![key.provider, key.owner, key.repo, rev],
    )?;
    let revision_rows = transaction.execute(
        "DELETE FROM shardline_revisions
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND revision = ?4",
        params![key.provider, key.owner, key.repo, rev],
    )?;
    transaction.commit()?;
    Ok(u64::try_from(revision_rows).unwrap_or(u64::MAX))
}

/// Deletes the oldest `prune_limit` revision rows for a repository (ordered
/// by created-at, then revision name) together with every tree entry of those
/// revisions, returning how many revision rows were removed.
///
/// The subqueries select the same oldest rows both times: the tree-entry
/// delete does not touch `shardline_revisions`, so the second subquery still
/// sees the full pre-prune row set. `prune_limit` is pre-computed by the
/// caller as `count - max_revisions` (never called when at/below the cap).
fn prune_revisions_over_cap_sql(
    connection: &Connection,
    key: &RepoKey,
    prune_limit: u64,
) -> Result<u64, LocalIndexStoreError> {
    let limit_i64 = i64::try_from(prune_limit)
        .map_err(|e| LocalIndexStoreError::IntegerOutOfRange(e.to_string()))?;
    let transaction = connection.unchecked_transaction()?;
    transaction.execute(
        "DELETE FROM shardline_tree_entries
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3
           AND revision IN (
               SELECT revision FROM shardline_revisions
               WHERE provider = ?1 AND owner = ?2 AND repo = ?3
               ORDER BY created_at_unix_seconds, revision
               LIMIT ?4
           )",
        params![key.provider, key.owner, key.repo, limit_i64],
    )?;
    let revision_rows = transaction.execute(
        "DELETE FROM shardline_revisions
         WHERE provider = ?1 AND owner = ?2 AND repo = ?3
           AND revision IN (
               SELECT revision FROM shardline_revisions
               WHERE provider = ?1 AND owner = ?2 AND repo = ?3
               ORDER BY created_at_unix_seconds, revision
               LIMIT ?4
           )",
        params![key.provider, key.owner, key.repo, limit_i64],
    )?;
    transaction.commit()?;
    Ok(u64::try_from(revision_rows).unwrap_or(u64::MAX))
}

fn list_revision_repo_keys_sql(
    connection: &Connection,
) -> Result<Vec<RepoKey>, LocalIndexStoreError> {
    let mut statement = connection.prepare(
        "SELECT DISTINCT provider, owner, repo
         FROM shardline_revisions
         ORDER BY provider, owner, repo",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(RepoKey {
            provider: row.get("provider")?,
            owner: row.get("owner")?,
            repo: row.get("repo")?,
        })
    })?;
    collect_rows(rows)
}

#[async_trait::async_trait]
impl TreeStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    async fn upsert_tree_entry(&self, entry: &TreeEntry) -> Result<TreeEntryOutcome, Self::Error> {
        let store = self.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            upsert_tree_entry_sql(&connection, &entry)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn tree_entry(
        &self,
        key: &TreeKey,
        path: &str,
    ) -> Result<Option<TreeEntry>, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            tree_entry_sql(&connection, &key, &path)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn delete_tree_entries(
        &self,
        key: &TreeKey,
        path: &str,
        recursive: bool,
    ) -> Result<u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            delete_tree_entries_sql(&connection, &key, &path, recursive)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn scan_tree(
        &self,
        key: &TreeKey,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TreeEntry>, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let prefix = prefix.to_owned();
        let cursor = cursor.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            scan_tree_sql(&connection, &key, &prefix, cursor.as_deref(), limit)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn upsert_revision(&self, rev: &RevisionRecord) -> Result<bool, Self::Error> {
        let store = self.clone();
        let rev = rev.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            upsert_revision_sql(&connection, &rev)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn revision(
        &self,
        key: &RepoKey,
        rev: &str,
    ) -> Result<Option<RevisionRecord>, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let rev = rev.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            revision_sql(&connection, &key, &rev)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn count_revisions(&self, key: &RepoKey) -> Result<u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            count_revisions_sql(&connection, &key)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn list_revisions(
        &self,
        key: &RepoKey,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let cursor = cursor.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            list_revisions_sql(&connection, &key, cursor.as_deref(), limit)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn delete_revision(&self, key: &RepoKey, rev: &str) -> Result<u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        let rev = rev.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            delete_revision_sql(&connection, &key, &rev)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn prune_revisions_over_cap(
        &self,
        key: &RepoKey,
        max_revisions: usize,
    ) -> Result<u64, Self::Error> {
        let store = self.clone();
        let key = key.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let count = count_revisions_sql(&connection, &key)?;
            let cap = u64::try_from(max_revisions).unwrap_or(u64::MAX);
            let Some(prune_limit) = count.checked_sub(cap) else {
                return Ok(0);
            };
            if prune_limit == 0 {
                return Ok(0);
            }
            prune_revisions_over_cap_sql(&connection, &key, prune_limit)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    async fn list_revision_repo_keys(&self) -> Result<Vec<RepoKey>, Self::Error> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            list_revision_repo_keys_sql(&connection)
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use super::*;

    fn make_store() -> LocalIndexStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalIndexStore::new(storage.path_buf()).expect("failed to create local index store")
    }

    fn key(revision: &str) -> TreeKey {
        TreeKey::new("github", "owner", "repo", revision)
    }

    fn repo_key() -> RepoKey {
        RepoKey::new("github", "owner", "repo")
    }

    fn entry(revision: &str, path: &str, file_id: &str, size: u64, updated: u64) -> TreeEntry {
        TreeEntry {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: revision.to_owned(),
            path: path.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes: size,
            updated_at_unix_seconds: updated,
        }
    }

    fn file_id(n: u8) -> String {
        format!("{:064x}", n)
    }

    #[tokio::test]
    async fn upsert_tree_entry_reports_created_flag() {
        let store = make_store();
        let e = entry("main", "a.txt", &file_id(1), 10, 100);
        let first = TreeStore::upsert_tree_entry(&store, &e).await.unwrap();
        assert!(first.created);
        let second = TreeStore::upsert_tree_entry(&store, &e).await.unwrap();
        assert!(!second.created);
    }

    #[tokio::test]
    async fn tree_entry_roundtrip_and_missing() {
        let store = make_store();
        let e = entry("main", "data/model.pt", &file_id(2), 123456, 1700000000);
        TreeStore::upsert_tree_entry(&store, &e).await.unwrap();
        let loaded = TreeStore::tree_entry(&store, &key("main"), "data/model.pt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, e);
        assert!(
            TreeStore::tree_entry(&store, &key("main"), "missing")
                .await
                .unwrap()
                .is_none()
        );
        // different revision is isolated
        assert!(
            TreeStore::tree_entry(&store, &key("feature"), "data/model.pt")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn scan_tree_returns_prefix_rows_ordered() {
        let store = make_store();
        let k = key("main");
        for (p, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
            ("zz.txt", 5),
        ] {
            TreeStore::upsert_tree_entry(&store, &entry("main", p, &file_id(id), 0, 0))
                .await
                .unwrap();
        }
        let rows = TreeStore::scan_tree(&store, &k, "", None, 100)
            .await
            .unwrap();
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "a.txt",
                "data/1.txt",
                "data/2.txt",
                "data/sub/3.txt",
                "zz.txt"
            ]
        );

        let rows = TreeStore::scan_tree(&store, &k, "data", None, 100)
            .await
            .unwrap();
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["data/1.txt", "data/2.txt", "data/sub/3.txt"]);

        // cursor resumes after the given raw path
        let rows = TreeStore::scan_tree(&store, &k, "", Some("data/2.txt"), 100)
            .await
            .unwrap();
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["data/sub/3.txt", "zz.txt"]);
    }

    #[tokio::test]
    async fn delete_tree_entries_exact_and_recursive() {
        let store = make_store();
        for (p, id) in [
            ("a.txt", 1),
            ("data/1.txt", 2),
            ("data/2.txt", 3),
            ("data/sub/3.txt", 4),
        ] {
            TreeStore::upsert_tree_entry(&store, &entry("main", p, &file_id(id), 0, 0))
                .await
                .unwrap();
        }
        let removed = TreeStore::delete_tree_entries(&store, &key("main"), "a.txt", false)
            .await
            .unwrap();
        assert_eq!(removed, 1);
        let removed = TreeStore::delete_tree_entries(&store, &key("main"), "missing", false)
            .await
            .unwrap();
        assert_eq!(removed, 0);

        let removed = TreeStore::delete_tree_entries(&store, &key("main"), "data", true)
            .await
            .unwrap();
        assert_eq!(removed, 3);
        assert!(
            TreeStore::scan_tree(&store, &key("main"), "", None, 100)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn revision_upsert_list_delete_cascades_tree_rows() {
        let store = make_store();
        let now = 1700000000u64;
        let rev = RevisionRecord {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: "feature".to_owned(),
            created_at_unix_seconds: now,
            updated_at_unix_seconds: now,
        };
        let created = TreeStore::upsert_revision(&store, &rev).await.unwrap();
        assert!(created);
        let created = TreeStore::upsert_revision(&store, &rev).await.unwrap();
        assert!(!created);

        let loaded = TreeStore::revision(&store, &repo_key(), "feature")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, rev);

        let listed = TreeStore::list_revisions(&store, &repo_key(), None, 100)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].revision, "feature");

        TreeStore::upsert_tree_entry(&store, &entry("feature", "x.txt", &file_id(9), 1, 2))
            .await
            .unwrap();
        let removed = TreeStore::delete_revision(&store, &repo_key(), "feature")
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(
            TreeStore::revision(&store, &repo_key(), "feature")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            TreeStore::scan_tree(&store, &key("feature"), "", None, 100)
                .await
                .unwrap()
                .is_empty()
        );

        let removed = TreeStore::delete_revision(&store, &repo_key(), "feature")
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn scan_tree_prefix_matches_nothing_returns_empty() {
        let store = make_store();
        TreeStore::upsert_tree_entry(&store, &entry("main", "a.txt", &file_id(1), 1, 1))
            .await
            .unwrap();
        let rows = TreeStore::scan_tree(&store, &key("main"), "nonexistent", None, 100)
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn scan_tree_respects_limit() {
        let store = make_store();
        for n in 0..5 {
            let id = (n + 1) as u8;
            TreeStore::upsert_tree_entry(
                &store,
                &entry("main", &format!("f{n}.txt"), &file_id(id), 1, 1),
            )
            .await
            .unwrap();
        }
        let rows = TreeStore::scan_tree(&store, &key("main"), "", None, 3)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        // Cursor resumes after the last returned raw path.
        let cursor = rows.last().unwrap().path.clone();
        let more = TreeStore::scan_tree(&store, &key("main"), "", Some(&cursor), 100)
            .await
            .unwrap();
        assert_eq!(more.len(), 2);
    }

    #[tokio::test]
    async fn delete_recursive_with_no_descendants_removes_nothing() {
        let store = make_store();
        TreeStore::upsert_tree_entry(&store, &entry("main", "a.txt", &file_id(1), 1, 1))
            .await
            .unwrap();
        let removed = TreeStore::delete_tree_entries(&store, &key("main"), "a.txt", true)
            .await
            .unwrap();
        assert_eq!(removed, 1);
        let removed = TreeStore::delete_tree_entries(&store, &key("main"), "missing/dir", true)
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn revision_missing_and_empty_list() {
        let store = make_store();
        assert!(
            TreeStore::revision(&store, &repo_key(), "nope")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            TreeStore::list_revisions(&store, &repo_key(), None, 100)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_revisions_respects_limit_and_cursor() {
        let store = make_store();
        for n in 0..5u8 {
            let rev = RevisionRecord {
                provider: "github".to_owned(),
                owner: "owner".to_owned(),
                repo: "repo".to_owned(),
                revision: format!("rev-{n:02}"),
                created_at_unix_seconds: u64::from(n),
                updated_at_unix_seconds: u64::from(n),
            };
            assert!(TreeStore::upsert_revision(&store, &rev).await.unwrap());
        }

        // A bounded listing returns at most `limit` rows, ordered by revision.
        let first = TreeStore::list_revisions(&store, &repo_key(), None, 3)
            .await
            .unwrap();
        let names: Vec<&str> = first.iter().map(|r| r.revision.as_str()).collect();
        assert_eq!(names, vec!["rev-00", "rev-01", "rev-02"]);

        // The cursor resumes after the last returned revision name.
        let cursor = first.last().unwrap().revision.clone();
        let rest = TreeStore::list_revisions(&store, &repo_key(), Some(&cursor), 100)
            .await
            .unwrap();
        let names: Vec<&str> = rest.iter().map(|r| r.revision.as_str()).collect();
        assert_eq!(names, vec!["rev-03", "rev-04"]);
    }

    #[tokio::test]
    async fn upsert_revision_returns_false_on_update() {
        let store = make_store();
        let rev = RevisionRecord {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: "main".to_owned(),
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        assert!(TreeStore::upsert_revision(&store, &rev).await.unwrap());
        let refreshed = RevisionRecord {
            updated_at_unix_seconds: 2,
            ..rev
        };
        assert!(
            !TreeStore::upsert_revision(&store, &refreshed)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn count_revisions_counts_only_the_matching_repo() {
        let store = make_store();
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            0
        );
        for n in 0..5u8 {
            let rev = RevisionRecord {
                provider: "github".to_owned(),
                owner: "owner".to_owned(),
                repo: "repo".to_owned(),
                revision: format!("rev-{n:02}"),
                created_at_unix_seconds: u64::from(n),
                updated_at_unix_seconds: u64::from(n),
            };
            assert!(TreeStore::upsert_revision(&store, &rev).await.unwrap());
        }
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            5
        );

        // A same-name upsert does not grow the count; a different repo is not
        // counted against this repository.
        let refreshed = RevisionRecord {
            provider: "github".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            revision: "rev-00".to_owned(),
            created_at_unix_seconds: 9,
            updated_at_unix_seconds: 9,
        };
        assert!(
            !TreeStore::upsert_revision(&store, &refreshed)
                .await
                .unwrap()
        );
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            5
        );
        let other = RepoKey::new("github", "owner", "other-repo");
        assert_eq!(TreeStore::count_revisions(&store, &other).await.unwrap(), 0);
    }

    async fn insert_revision_record(
        store: &LocalIndexStore,
        provider: &str,
        owner: &str,
        repo: &str,
        revision: &str,
        created_at: u64,
    ) {
        let rev = RevisionRecord {
            provider: provider.to_owned(),
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            revision: revision.to_owned(),
            created_at_unix_seconds: created_at,
            updated_at_unix_seconds: created_at,
        };
        assert!(TreeStore::upsert_revision(store, &rev).await.unwrap());
    }

    #[tokio::test]
    async fn prune_revisions_over_cap_removes_oldest_down_to_cap() {
        let store = make_store();
        // created_at is deliberately NOT aligned with the name order, so the
        // prune must follow created-at (oldest first), not the name order that
        // `list_revisions` uses for pagination.
        insert_revision_record(&store, "github", "owner", "repo", "rev-b", 200).await;
        insert_revision_record(&store, "github", "owner", "repo", "rev-a", 100).await;
        insert_revision_record(&store, "github", "owner", "repo", "rev-c", 300).await;
        insert_revision_record(&store, "github", "owner", "repo", "rev-e", 500).await;
        insert_revision_record(&store, "github", "owner", "repo", "rev-d", 400).await;
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            5
        );

        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), 2)
            .await
            .unwrap();
        assert_eq!(removed, 3);
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            2
        );
        // The three oldest (rev-a@100, rev-b@200, rev-c@300) were evicted; the
        // two newest-created (rev-d@400, rev-e@500) survive regardless of name.
        let remaining = TreeStore::list_revisions(&store, &repo_key(), None, 100)
            .await
            .unwrap();
        let names: Vec<&str> = remaining.iter().map(|r| r.revision.as_str()).collect();
        assert_eq!(names, vec!["rev-d", "rev-e"]);
    }

    #[tokio::test]
    async fn prune_revisions_over_cap_ties_break_by_name_and_cascade_tree_rows() {
        let store = make_store();
        // Same created_at for every row: the name tiebreaker decides, so
        // rev-00..rev-02 are the oldest and must be evicted first.
        for n in 0..5u8 {
            insert_revision_record(&store, "github", "owner", "repo", &format!("rev-{n:02}"), 7)
                .await;
        }
        // Tree rows for the to-be-pruned revisions must cascade away.
        for n in 0..5u8 {
            TreeStore::upsert_tree_entry(
                &store,
                &entry(&format!("rev-{n:02}"), "x.txt", &file_id(n + 1), 1, 1),
            )
            .await
            .unwrap();
        }
        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), 3)
            .await
            .unwrap();
        assert_eq!(removed, 2);
        let remaining = TreeStore::list_revisions(&store, &repo_key(), None, 100)
            .await
            .unwrap();
        let names: Vec<&str> = remaining.iter().map(|r| r.revision.as_str()).collect();
        assert_eq!(names, vec!["rev-02", "rev-03", "rev-04"]);
        // The evicted revisions' tree rows are gone; the survivors' remain.
        assert!(
            TreeStore::scan_tree(&store, &key("rev-00"), "", None, 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            TreeStore::scan_tree(&store, &key("rev-01"), "", None, 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            TreeStore::scan_tree(&store, &key("rev-02"), "", None, 100)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn prune_revisions_over_cap_leaves_at_cap_and_below_untouched() {
        let store = make_store();
        for n in 0..3u8 {
            insert_revision_record(
                &store,
                "github",
                "owner",
                "repo",
                &format!("rev-{n:02}"),
                u64::from(n),
            )
            .await;
        }
        // At exactly the cap: nothing to prune.
        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), 3)
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            3
        );
        // Below the cap: nothing to prune.
        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), 10)
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(
            TreeStore::count_revisions(&store, &repo_key())
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn prune_revisions_over_cap_leaves_other_repos_untouched() {
        let store = make_store();
        for n in 0..5u8 {
            insert_revision_record(
                &store,
                "github",
                "owner",
                "repo",
                &format!("rev-{n:02}"),
                u64::from(n),
            )
            .await;
        }
        let other = RepoKey::new("github", "owner", "other-repo");
        insert_revision_record(&store, "github", "owner", "other-repo", "main", 1).await;

        let removed = TreeStore::prune_revisions_over_cap(&store, &repo_key(), 2)
            .await
            .unwrap();
        assert_eq!(removed, 3);
        // The other repo is untouched.
        assert_eq!(TreeStore::count_revisions(&store, &other).await.unwrap(), 1);
        let other_list = TreeStore::list_revisions(&store, &other, None, 100)
            .await
            .unwrap();
        assert_eq!(other_list[0].revision, "main");
    }

    #[tokio::test]
    async fn list_revision_repo_keys_returns_distinct_repos_ordered() {
        let store = make_store();
        assert!(
            TreeStore::list_revision_repo_keys(&store)
                .await
                .unwrap()
                .is_empty()
        );
        insert_revision_record(&store, "github", "owner", "repo", "main", 1).await;
        insert_revision_record(&store, "github", "owner", "repo", "feature", 2).await;
        insert_revision_record(&store, "github", "owner", "other-repo", "main", 3).await;
        insert_revision_record(&store, "gitlab", "team", "assets", "main", 4).await;

        let keys = TreeStore::list_revision_repo_keys(&store).await.unwrap();
        assert_eq!(
            keys,
            vec![
                RepoKey::new("github", "owner", "other-repo"),
                RepoKey::new("github", "owner", "repo"),
                RepoKey::new("gitlab", "team", "assets"),
            ]
        );
    }
}
