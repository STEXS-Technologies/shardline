use std::path::Path;

use rusqlite::{Connection, params};
use shardline_protocol::unix_now_seconds_lossy;

use crate::{
    hub::{HubFileEntry, HubRepo, HubRepoType, HubRevision, HubStore, HubWebhook},
    local_sqlite::{LocalIndexStore, LocalIndexStoreError, i64_to_u64, u64_to_i64},
};

fn sqlite_store_error(error: &LocalIndexStoreError) -> rusqlite::Error {
    rusqlite::Error::InvalidParameterName(error.to_string())
}

/// Opens a read-only connection to the hub SQLite database.
///
/// Each call opens a new connection because `rusqlite::Connection` is `!Send` and
/// cannot be cached across threads. SQLite file opens are fast for local files,
/// so per-call overhead is acceptable. Using `SQLITE_OPEN_NO_MUTEX` avoids
/// unnecessary locking for single-writer access patterns.
fn open_hub_connection(root: &Path) -> Result<Connection, LocalIndexStoreError> {
    let database_path = root.join("metadata.sqlite3");
    let connection = Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    Ok(connection)
}

/// Opens a read-write connection to the hub SQLite database.
///
/// Same constraints as [`open_hub_connection`]: `rusqlite::Connection` is `!Send`,
/// so connections are opened per-call rather than cached. `SQLITE_OPEN_NO_MUTEX`
/// is safe here because only one writer operates at a time within each method.
fn open_hub_connection_rw(root: &Path) -> Result<Connection, LocalIndexStoreError> {
    let database_path = root.join("metadata.sqlite3");
    let connection = Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    Ok(connection)
}

/// Ensures the hub SQLite tables exist in the given root directory.
/// Creates the database file and tables if they don't exist.
pub fn ensure_hub_tables(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = root.join("metadata.sqlite3");
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
            repo_id TEXT PRIMARY KEY, repo_type TEXT NOT NULL, private INTEGER NOT NULL DEFAULT 0,
            default_branch TEXT NOT NULL, created_at_unix_seconds INTEGER NOT NULL,
            updated_at_unix_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
            repo_id TEXT NOT NULL, ref_name TEXT NOT NULL, sha TEXT NOT NULL,
            parent_sha TEXT, message TEXT, created_at_unix_seconds INTEGER NOT NULL,
            PRIMARY KEY (repo_id, sha)
        );
        CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
            ON shardline_hub_revisions (repo_id, ref_name);
        CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
            commit_sha TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL,
            sha TEXT NOT NULL, is_lfs INTEGER NOT NULL DEFAULT 0, inline_content BLOB,
            PRIMARY KEY (commit_sha, path)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
            oid TEXT PRIMARY KEY, data BLOB NOT NULL, created_at_unix_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT NOT NULL,
            url TEXT NOT NULL, events TEXT NOT NULL, secret TEXT,
            created_at_unix_seconds INTEGER NOT NULL
        );"
    )?;
    Ok(())
}

const fn repo_type_to_str(t: HubRepoType) -> &'static str {
    t.as_str()
}

impl HubStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;
        let now = unix_now_seconds_lossy();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3".to_owned();
        conn.execute(
            "INSERT INTO shardline_hub_repos (repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![name, repo_type_to_str(repo_type), private as i64, initial_sha, u64_to_i64(now)?, u64_to_i64(now)?],
        )?;
        // Insert initial revision
        conn.execute(
            "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
             VALUES (?1, 'main', ?2, NULL, NULL, ?3)",
            params![name, initial_sha, u64_to_i64(now)?],
        )?;
        Ok(HubRepo {
            repo_id: name.to_owned(),
            repo_type,
            private,
            default_branch: initial_sha,
            created_at_unix_seconds: now,
        })
    }

    fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let result = conn
            .query_row(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
                 FROM shardline_hub_repos WHERE repo_id = ?1",
                params![repo_id],
                |row| {
                    let rt_str: String = row.get(1)?;
                    let repo_type = HubRepoType::parse_str(&rt_str)
                        .ok_or_else(|| rusqlite::Error::InvalidParameterName(rt_str.clone()))?;
                    Ok(HubRepo {
                        repo_id: row.get(0)?,
                        repo_type,
                        private: row.get::<_, i64>(2)? != 0,
                        default_branch: row.get(3)?,
                        created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(4)?)
                            .map_err(|e| sqlite_store_error(&e))?,
                    })
                },
            )
            .optional()?;
        Ok(result)
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let mut stmt = conn.prepare(
            "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
             FROM shardline_hub_repos ORDER BY repo_id",
        )?;
        let rows = stmt.query_map([], |row| {
            let rt_str: String = row.get(1)?;
            let repo_type = HubRepoType::parse_str(&rt_str)
                .ok_or_else(|| rusqlite::Error::InvalidParameterName(rt_str.clone()))?;
            Ok(HubRepo {
                repo_id: row.get(0)?,
                repo_type,
                private: row.get::<_, i64>(2)? != 0,
                default_branch: row.get(3)?,
                created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(4)?)
                    .map_err(|e| sqlite_store_error(&e))?,
            })
        })?;
        let mut repos = Vec::new();
        for row in rows {
            repos.push(row?);
        }
        Ok(repos)
    }

    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let pattern = format!("{name_prefix}%");
        let mut repos = Vec::new();
        if let Some(rt) = repo_type {
            let rt_str = rt.as_str();
            let mut stmt = conn.prepare(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
                 FROM shardline_hub_repos
                 WHERE repo_id LIKE ?1 AND repo_type = ?2
                 ORDER BY repo_id LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![pattern, rt_str, limit as i64], |row| {
                let repo_type_str: String = row.get(1)?;
                let parsed_repo_type = HubRepoType::parse_str(&repo_type_str)
                    .ok_or_else(|| rusqlite::Error::InvalidParameterName(repo_type_str.clone()))?;
                Ok(HubRepo {
                    repo_id: row.get(0)?,
                    repo_type: parsed_repo_type,
                    private: row.get::<_, i64>(2)? != 0,
                    default_branch: row.get(3)?,
                    created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(4)?)
                        .map_err(|e| sqlite_store_error(&e))?,
                })
            })?;
            for row in rows {
                repos.push(row?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
                 FROM shardline_hub_repos
                 WHERE repo_id LIKE ?1
                 ORDER BY repo_id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![pattern, limit as i64], |row| {
                let repo_type_str: String = row.get(1)?;
                let parsed_repo_type = HubRepoType::parse_str(&repo_type_str)
                    .ok_or_else(|| rusqlite::Error::InvalidParameterName(repo_type_str.clone()))?;
                Ok(HubRepo {
                    repo_id: row.get(0)?,
                    repo_type: parsed_repo_type,
                    private: row.get::<_, i64>(2)? != 0,
                    default_branch: row.get(3)?,
                    created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(4)?)
                        .map_err(|e| sqlite_store_error(&e))?,
                })
            })?;
            for row in rows {
                repos.push(row?);
            }
        }
        Ok(repos)
    }

    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;

        // Optimistic concurrency check
        if let Some(parent) = parent_sha {
            let current_head: Option<String> = conn
                .query_row(
                    "SELECT default_branch FROM shardline_hub_repos WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()?;
            match current_head {
                Some(head) if head != parent => {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                None => {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                _ => {}
            }
        }

        let now = unix_now_seconds_lossy();

        // Update repo HEAD
        conn.execute(
            "UPDATE shardline_hub_repos SET default_branch = ?1, updated_at_unix_seconds = ?2
             WHERE repo_id = ?3",
            params![new_sha, u64_to_i64(now)?, repo_id],
        )?;

        // Insert revision
        conn.execute(
            "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![repo_id, ref_name, new_sha, parent_sha, message, u64_to_i64(now)?],
        )?;

        Ok(HubRevision {
            repo_id: repo_id.to_owned(),
            ref_name: ref_name.to_owned(),
            sha: new_sha.to_owned(),
            parent_sha: parent_sha.map(ToOwned::to_owned),
            message: Some(message.to_owned()),
            created_at_unix_seconds: now,
        })
    }

    fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let mut stmt = conn.prepare(
            "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
             FROM shardline_hub_revisions WHERE repo_id = ?1 ORDER BY created_at_unix_seconds DESC, rowid DESC",
        )?;
        let rows = stmt.query_map(params![repo_id], |row| {
            Ok(HubRevision {
                repo_id: row.get(0)?,
                ref_name: row.get(1)?,
                sha: row.get(2)?,
                parent_sha: row.get(3)?,
                message: row.get(4)?,
                created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(5)?)
                    .map_err(|e| sqlite_store_error(&e))?,
            })
        })?;
        let mut revisions = Vec::new();
        for row in rows {
            revisions.push(row?);
        }
        Ok(revisions)
    }

    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Self::Error> {
        let conn = open_hub_connection(self.root())?;

        if revision.is_empty() || revision == "main" {
            let head: Option<String> = conn
                .query_row(
                    "SELECT default_branch FROM shardline_hub_repos WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()?;
            return Ok(head);
        }

        // Direct SHA match
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_revisions WHERE repo_id = ?1 AND sha = ?2)",
            params![repo_id, revision],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(Some(revision.to_owned()));
        }

        // Ref name match
        let sha: Option<String> = conn
            .query_row(
                "SELECT sha FROM shardline_hub_revisions WHERE repo_id = ?1 AND ref_name = ?2
                 ORDER BY created_at_unix_seconds DESC LIMIT 1",
                params![repo_id, revision],
                |row| row.get(0),
            )
            .optional()?;
        Ok(sha)
    }

    fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO shardline_hub_file_entries (commit_sha, path, size, sha, is_lfs, inline_content)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for file in files {
            stmt.execute(params![
                commit_sha,
                file.path,
                u64_to_i64(file.size)?,
                file.sha,
                file.is_lfs as i64,
                file.inline_content,
            ])?;
        }
        Ok(())
    }

    fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let mut stmt = conn.prepare(
            "SELECT path, size, sha, is_lfs, inline_content FROM shardline_hub_file_entries
             WHERE commit_sha = ?1 ORDER BY path",
        )?;
        let rows = stmt.query_map(params![commit_sha], |row| {
            Ok(HubFileEntry {
                path: row.get(0)?,
                size: i64_to_u64(row.get::<_, i64>(1)?).map_err(|e| sqlite_store_error(&e))?,
                sha: row.get(2)?,
                is_lfs: row.get::<_, i64>(3)? != 0,
                inline_content: row.get(4)?,
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    fn put_lfs_object(&self, oid: &str, data: &[u8]) -> Result<(), Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;
        let now = unix_now_seconds_lossy();
        conn.execute(
            "INSERT OR REPLACE INTO shardline_hub_lfs_objects (oid, data, size, created_at_unix_seconds)
             VALUES (?1, ?2, ?3, ?4)",
            params![oid, data, i64::try_from(data.len()).map_err(|_e| LocalIndexStoreError::IntegerOutOfRange)?, u64_to_i64(now)?],
        )?;
        Ok(())
    }

    fn get_lfs_object(&self, oid: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let result = conn
            .query_row(
                "SELECT data FROM shardline_hub_lfs_objects WHERE oid = ?1",
                params![oid],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        Ok(result)
    }

    fn has_lfs_object(&self, oid: &str) -> Result<bool, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_lfs_objects WHERE oid = ?1)",
            params![oid],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;
        let now = unix_now_seconds_lossy();
        let counter: u64 = {
            let row: Option<u64> = conn
                .query_row(
                    "SELECT COUNT(*) FROM shardline_hub_webhooks WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get(0),
                )
                .optional()?;
            row.unwrap_or(0)
        };
        let id = format!("wh-{}-{}", now, counter);
        let events_str = events.join(",");
        conn.execute(
            "INSERT INTO shardline_hub_webhooks (id, repo_id, url, events, secret, active, created_at_unix_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![id, repo_id, url, events_str, secret, u64_to_i64(now)?],
        )?;
        Ok(HubWebhook {
            id,
            repo_id: repo_id.to_owned(),
            url: url.to_owned(),
            events: events.to_vec(),
            secret: secret.map(ToOwned::to_owned),
            active: true,
            created_at_unix_seconds: now,
        })
    }

    fn list_webhooks(&self, repo_id: &str) -> Result<Vec<HubWebhook>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, url, events, secret, active, created_at_unix_seconds
             FROM shardline_hub_webhooks WHERE repo_id = ?1",
        )?;
        let rows = stmt.query_map(params![repo_id], |row| {
            let events_str: String = row.get(3)?;
            let active: i64 = row.get(5)?;
            Ok(HubWebhook {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                url: row.get(2)?,
                events: events_str.split(',').map(ToOwned::to_owned).collect(),
                secret: row.get(4)?,
                active: active != 0,
                created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(6)?)
                    .map_err(|e| sqlite_store_error(&e))?,
            })
        })?;
        let mut webhooks = Vec::new();
        for row in rows {
            webhooks.push(row?);
        }
        Ok(webhooks)
    }

    fn delete_webhook(&self, repo_id: &str, webhook_id: &str) -> Result<(), Self::Error> {
        let conn = open_hub_connection_rw(self.root())?;
        conn.execute(
            "DELETE FROM shardline_hub_webhooks WHERE repo_id = ?1 AND id = ?2",
            params![repo_id, webhook_id],
        )?;
        Ok(())
    }

    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<HubWebhook>, Self::Error> {
        let conn = open_hub_connection(self.root())?;
        let mut stmt = conn.prepare(
            "SELECT id, repo_id, url, events, secret, active, created_at_unix_seconds
             FROM shardline_hub_webhooks
             WHERE repo_id = ?1 AND active = 1 AND (',' || events || ',') LIKE ('%' || ?2 || '%')",
        )?;
        let rows = stmt.query_map(params![repo_id, event], |row| {
            let events_str: String = row.get(3)?;
            let active: i64 = row.get(5)?;
            Ok(HubWebhook {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                url: row.get(2)?,
                events: events_str.split(',').map(ToOwned::to_owned).collect(),
                secret: row.get(4)?,
                active: active != 0,
                created_at_unix_seconds: i64_to_u64(row.get::<_, i64>(6)?)
                    .map_err(|e| sqlite_store_error(&e))?,
            })
        })?;
        let mut webhooks = Vec::new();
        for row in rows {
            webhooks.push(row?);
        }
        Ok(webhooks)
    }
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{BoxedHubStore, HubRepoType};

    fn make_store() -> (shardline_test_support::TempStorage, LocalIndexStore) {
        let ts = shardline_test_support::TempStorage::new();
        let root = ts.path();

        // Manually create database and apply hub migrations.
        // We cannot use LocalIndexStore::new() on macOS because it uses
        // SQLITE_OPEN_NOFOLLOW and /var/folders/... contains symlinks.
        let db_path = root.join("metadata.sqlite3");
        let conn = Connection::open(&db_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
                repo_id TEXT PRIMARY KEY,
                repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
                private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
                default_branch TEXT NOT NULL,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                parent_sha TEXT,
                message TEXT,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                PRIMARY KEY (repo_id, sha),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
                ON shardline_hub_revisions (repo_id, ref_name);
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                sha TEXT NOT NULL,
                is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),
                inline_content BLOB,
                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
                oid TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                url TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT 'push',
                secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_webhooks_repo_idx ON shardline_hub_webhooks (repo_id);",
        )
        .expect("create hub tables");
        drop(conn);

        let store = LocalIndexStore::open(root.to_path_buf());
        (ts, store)
    }

    #[test]
    fn create_and_get_repo() {
        let (_ts, store) = make_store();

        let repo = store
            .create_repo(HubRepoType::Model, "org/model", false)
            .expect("create_repo");

        assert_eq!(repo.repo_id, "org/model");
        assert_eq!(repo.repo_type, HubRepoType::Model);
        assert!(!repo.private);

        let fetched = store.get_repo("org/model").expect("get_repo");
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.repo_id, "org/model");
        assert_eq!(fetched.repo_type, HubRepoType::Model);
    }

    #[test]
    fn get_repo_returns_none_for_missing() {
        let (_ts, store) = make_store();
        let result = store.get_repo("nope/nope").expect("get_repo");
        assert!(result.is_none());
    }

    #[test]
    fn list_repos_is_empty_initially() {
        let (_ts, store) = make_store();
        let repos = store.list_repos().expect("list_repos");
        assert!(repos.is_empty());
    }

    #[test]
    fn list_repos_returns_all_in_alphabetical_order() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Space, "z/space", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "a/model", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "m/dataset", false)
            .unwrap();

        let repos = store.list_repos().expect("list_repos");
        assert_eq!(repos.len(), 3);
        assert_eq!(repos[0].repo_id, "a/model");
        assert_eq!(repos[1].repo_id, "m/dataset");
        assert_eq!(repos[2].repo_id, "z/space");
    }

    #[test]
    fn search_repos_by_name_prefix() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model-a", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "org/model-b", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "org/dataset", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "other/model", false)
            .unwrap();

        let results = store.search_repos(None, "org/", 10).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.repo_id.starts_with("org/")));
    }

    #[test]
    fn search_repos_by_type_filter() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "a/model", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Dataset, "b/dataset", false)
            .unwrap();
        store
            .create_repo(HubRepoType::Model, "c/model", false)
            .unwrap();

        let results = store
            .search_repos(Some(HubRepoType::Model), "", 10)
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.repo_type == HubRepoType::Model));
    }

    #[test]
    fn search_repos_respects_limit() {
        let (_ts, store) = make_store();
        for i in 0..10 {
            store
                .create_repo(HubRepoType::Model, &format!("repo-{i:02}"), false)
                .unwrap();
        }

        let results = store.search_repos(None, "repo-", 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].repo_id, "repo-00");
        assert_eq!(results[2].repo_id, "repo-02");
    }

    #[test]
    fn create_repo_duplicate_fails() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();
        let result = store.create_repo(HubRepoType::Dataset, "org/model", true);
        assert!(result.is_err());
    }

    #[test]
    fn create_repo_stores_private_flag() {
        let (_ts, store) = make_store();
        let repo = store
            .create_repo(HubRepoType::Dataset, "org/private", true)
            .unwrap();
        assert!(repo.private);

        let fetched = store.get_repo("org/private").unwrap().unwrap();
        assert!(fetched.private);
    }

    #[test]
    fn create_repo_all_repo_types() {
        let (_ts, store) = make_store();

        let model = store.create_repo(HubRepoType::Model, "m1", false).unwrap();
        assert_eq!(model.repo_type, HubRepoType::Model);

        let ds = store
            .create_repo(HubRepoType::Dataset, "d1", false)
            .unwrap();
        assert_eq!(ds.repo_type, HubRepoType::Dataset);

        let space = store.create_repo(HubRepoType::Space, "s1", false).unwrap();
        assert_eq!(space.repo_type, HubRepoType::Space);
    }

    #[test]
    fn create_revision_initial_no_parent() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let rev = store
            .create_revision("org/model", None, "abc123", "main", "initial commit")
            .expect("create_revision");

        assert_eq!(rev.sha, "abc123");
        assert_eq!(rev.ref_name, "main");
        assert!(rev.parent_sha.is_none());
        assert_eq!(rev.message.as_deref(), Some("initial commit"));
    }

    #[test]
    fn create_revision_with_parent_succeeds() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        let rev2 = store
            .create_revision(
                "org/model",
                Some(initial_sha),
                "def456",
                "main",
                "second commit",
            )
            .expect("create_revision with parent");

        assert_eq!(rev2.parent_sha.as_deref(), Some(initial_sha));
        assert_eq!(rev2.sha, "def456");
    }

    #[test]
    fn create_revision_wrong_parent_fails() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let result = store.create_revision(
            "org/model",
            Some("wrong_sha"),
            "def456",
            "main",
            "should fail",
        );
        assert!(result.is_err());
    }

    #[test]
    fn create_revision_on_nonexistent_repo_fails() {
        let (_ts, store) = make_store();
        let result = store.create_revision("nope", None, "abc", "main", "msg");
        assert!(result.is_err());
    }

    #[test]
    fn list_revisions_returns_all_revisions() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        store
            .create_revision("org/model", Some(initial_sha), "aaa", "main", "first")
            .unwrap();
        store
            .create_revision("org/model", Some("aaa"), "bbb", "main", "second")
            .unwrap();
        store
            .create_revision("org/model", Some("bbb"), "ccc", "dev", "third")
            .unwrap();

        let revs = store.list_revisions("org/model").expect("list_revisions");
        assert_eq!(revs.len(), 4); // 1 initial + 3 added
        // All SHAs present
        let shas: Vec<&str> = revs.iter().map(|r| r.sha.as_str()).collect();
        assert!(shas.contains(&"aaa"));
        assert!(shas.contains(&"bbb"));
        assert!(shas.contains(&"ccc"));
        assert!(shas.contains(&initial_sha));
    }

    #[test]
    fn resolve_revision_main_returns_default_branch() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let sha = store
            .resolve_revision("org/model", "main")
            .expect("resolve main");
        assert!(sha.is_some());
        // default_branch is the initial empty tree sha
        assert_eq!(sha.unwrap(), "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3");
    }

    #[test]
    fn resolve_revision_empty_string_returns_head() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let sha = store
            .resolve_revision("org/model", "")
            .expect("resolve empty");
        assert!(sha.is_some());
    }

    #[test]
    fn resolve_revision_by_sha() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();
        store
            .create_revision("org/model", None, "abc123", "main", "commit")
            .unwrap();

        let sha = store
            .resolve_revision("org/model", "abc123")
            .expect("resolve sha");
        assert_eq!(sha.as_deref(), Some("abc123"));
    }

    #[test]
    fn resolve_revision_by_ref_name() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();
        store
            .create_revision("org/model", None, "abc123", "feature", "commit")
            .unwrap();

        let sha = store
            .resolve_revision("org/model", "feature")
            .expect("resolve ref");
        assert_eq!(sha.as_deref(), Some("abc123"));
    }

    #[test]
    fn resolve_revision_nonexistent_returns_none() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        let sha = store
            .resolve_revision("org/model", "nonexistent")
            .expect("resolve");
        assert!(sha.is_none());
    }

    #[test]
    fn store_and_get_files() {
        let (_ts, store) = make_store();

        let files = vec![
            HubFileEntry {
                path: "a.txt".into(),
                size: 100,
                sha: "sha_a".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "b.bin".into(),
                size: 2048,
                sha: "sha_b".into(),
                is_lfs: true,
                inline_content: None,
            },
            HubFileEntry {
                path: "c/d.txt".into(),
                size: 50,
                sha: "sha_c".into(),
                is_lfs: false,
                inline_content: None,
            },
        ];

        store.store_files("commit1", &files).expect("store_files");
        let retrieved = store.get_files("commit1").expect("get_files");

        assert_eq!(retrieved.len(), 3);
        // Sorted by path
        assert_eq!(retrieved[0].path, "a.txt");
        assert_eq!(retrieved[0].size, 100);
        assert!(!retrieved[0].is_lfs);
        assert_eq!(retrieved[1].path, "b.bin");
        assert!(retrieved[1].is_lfs);
        assert_eq!(retrieved[2].path, "c/d.txt");
    }

    #[test]
    fn store_files_empty() {
        let (_ts, store) = make_store();
        store
            .store_files("empty_commit", &[])
            .expect("store_files empty");
        let files = store.get_files("empty_commit").expect("get_files");
        assert!(files.is_empty());
    }

    #[test]
    fn store_files_overwrites_existing() {
        let (_ts, store) = make_store();

        let v1 = vec![HubFileEntry {
            path: "f.txt".into(),
            size: 10,
            sha: "old".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let v2 = vec![HubFileEntry {
            path: "f.txt".into(),
            size: 20,
            sha: "new".into(),
            is_lfs: true,
            inline_content: None,
        }];

        store.store_files("c1", &v1).unwrap();
        store.store_files("c1", &v2).unwrap();

        let files = store.get_files("c1").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].sha, "new");
        assert_eq!(files[0].size, 20);
        assert!(files[0].is_lfs);
    }

    #[test]
    fn put_and_get_lfs_object() {
        let (_ts, store) = make_store();

        let data = b"hello lfs content";
        store
            .put_lfs_object("oid_abc", data)
            .expect("put_lfs_object");

        let retrieved = store.get_lfs_object("oid_abc").expect("get_lfs_object");
        assert_eq!(retrieved.as_deref(), Some(data as &[u8]));
    }

    #[test]
    fn get_lfs_object_nonexistent_returns_none() {
        let (_ts, store) = make_store();
        let result = store.get_lfs_object("nope").expect("get_lfs_object");
        assert!(result.is_none());
    }

    #[test]
    fn has_lfs_object_true() {
        let (_ts, store) = make_store();
        store.put_lfs_object("oid_1", b"data").unwrap();
        assert!(store.has_lfs_object("oid_1").expect("has_lfs_object"));
    }

    #[test]
    fn has_lfs_object_false() {
        let (_ts, store) = make_store();
        assert!(!store.has_lfs_object("nope").expect("has_lfs_object"));
    }

    #[test]
    fn put_lfs_object_overwrites() {
        let (_ts, store) = make_store();
        store.put_lfs_object("oid", b"old").unwrap();
        store.put_lfs_object("oid", b"new").unwrap();

        let data = store.get_lfs_object("oid").unwrap().unwrap();
        assert_eq!(&data, b"new");
    }

    #[test]
    fn lfs_object_large_data() {
        let (_ts, store) = make_store();
        let large = vec![0xABu8; 1024 * 1024]; // 1 MB
        store.put_lfs_object("large_oid", &large).unwrap();

        let retrieved = store.get_lfs_object("large_oid").unwrap().unwrap();
        assert_eq!(retrieved.len(), 1024 * 1024);
        assert!(retrieved.iter().all(|&b| b == 0xAB));
    }

    // === Full lifecycle: create repo → commit → files → LFS ===

    #[test]
    fn full_lifecycle_single_commit() {
        let (_ts, store) = make_store();

        // Create repo
        let repo = store
            .create_repo(HubRepoType::Model, "org/llm", false)
            .unwrap();
        assert_eq!(repo.repo_id, "org/llm");

        // Initial commit (already created by create_repo)
        let revs = store.list_revisions("org/llm").unwrap();
        assert_eq!(revs.len(), 1);
        let initial_sha = revs[0].sha.clone();

        // Store files at initial commit
        let files = vec![
            HubFileEntry {
                path: "README.md".into(),
                size: 256,
                sha: "sha_readme".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "model.bin".into(),
                size: 5_000_000,
                sha: "sha_model".into(),
                is_lfs: true,
                inline_content: None,
            },
        ];
        store.store_files(&initial_sha, &files).unwrap();

        // Store LFS object
        store
            .put_lfs_object("sha_model", b"model weights data")
            .unwrap();

        // Second commit
        let new_sha = "abc123def456";
        store
            .create_revision("org/llm", Some(&initial_sha), new_sha, "main", "add model")
            .unwrap();

        // Verify HEAD updated
        let repo = store.get_repo("org/llm").unwrap().unwrap();
        assert_eq!(repo.default_branch, new_sha);

        // Verify both revisions exist
        let revs = store.list_revisions("org/llm").unwrap();
        assert_eq!(revs.len(), 2);
    }

    #[test]
    fn full_lifecycle_multi_branch() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Dataset, "org/data", false)
            .unwrap();
        let _initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // main branch commit
        store
            .create_revision(
                "org/data",
                Some("4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3"),
                "main_sha",
                "main",
                "init",
            )
            .unwrap();

        // feature branch from main commit
        store
            .create_revision(
                "org/data",
                Some("main_sha"),
                "feat_sha",
                "feature",
                "feature work",
            )
            .unwrap();

        // Both revisions exist (1 initial + 2 commits)
        let revs = store.list_revisions("org/data").unwrap();
        assert_eq!(revs.len(), 3);

        // Resolve feature branch
        let sha = store.resolve_revision("org/data", "feature").unwrap();
        assert_eq!(sha.as_deref(), Some("feat_sha"));

        // Resolve main branch by SHA
        let sha = store.resolve_revision("org/data", "main_sha").unwrap();
        assert_eq!(sha.as_deref(), Some("main_sha"));
    }

    // === BoxedHubStore type-erased wrapper tests ===

    #[test]
    fn boxed_hub_store_create_and_get_repo() {
        let (_ts, store) = make_store();
        let boxed = BoxedHubStore::from_store(store);

        let repo = boxed
            .create_repo(HubRepoType::Space, "org/space", true)
            .expect("create_repo via boxed");
        assert_eq!(repo.repo_id, "org/space");
        assert!(repo.private);

        let fetched = boxed.get_repo("org/space").expect("get_repo via boxed");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().repo_id, "org/space");
    }

    #[test]
    fn boxed_hub_store_revision_and_files() {
        let (_ts, store) = make_store();
        let boxed = BoxedHubStore::from_store(store);

        boxed
            .create_repo(HubRepoType::Model, "org/m", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Create revision via boxed
        let rev = boxed
            .create_revision("org/m", None, "rev1", "main", "first")
            .unwrap();
        assert_eq!(rev.sha, "rev1");

        // Store files via boxed
        let files = vec![HubFileEntry {
            path: "test.py".into(),
            size: 42,
            sha: "sha_py".into(),
            is_lfs: false,
            inline_content: None,
        }];
        boxed.store_files("rev1", &files).unwrap();

        let retrieved = boxed.get_files("rev1").unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].path, "test.py");
    }

    #[test]
    fn boxed_hub_store_lfs() {
        let (_ts, store) = make_store();
        let boxed = BoxedHubStore::from_store(store);

        boxed.put_lfs_object("oid1", b"content").unwrap();
        assert!(boxed.has_lfs_object("oid1").unwrap());
        assert!(!boxed.has_lfs_object("oid2").unwrap());

        let data = boxed.get_lfs_object("oid1").unwrap().unwrap();
        assert_eq!(&data, b"content");
    }

    #[test]
    fn boxed_hub_store_list_repos_and_revisions() {
        let (_ts, store) = make_store();
        let boxed = BoxedHubStore::from_store(store);

        assert!(boxed.list_repos().unwrap().is_empty());

        boxed.create_repo(HubRepoType::Model, "a", false).unwrap();
        boxed.create_repo(HubRepoType::Dataset, "b", false).unwrap();

        let repos = boxed.list_repos().unwrap();
        assert_eq!(repos.len(), 2);

        let revs = boxed.list_revisions("a").unwrap();
        assert_eq!(revs.len(), 1); // Initial revision from create_repo
    }

    #[test]
    fn boxed_hub_store_resolve_revision() {
        let (_ts, store) = make_store();
        let boxed = BoxedHubStore::from_store(store);

        boxed
            .create_repo(HubRepoType::Model, "org/m", false)
            .unwrap();

        let sha = boxed.resolve_revision("org/m", "main").unwrap();
        assert!(sha.is_some());
    }

    // === Edge cases ===

    #[test]
    fn empty_commit_sha_no_files() {
        let (_ts, store) = make_store();
        let files = store.get_files("nonexistent_sha").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn sequential_commits_on_single_branch() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/m", false)
            .unwrap();

        // create_repo inserts initial revision with SHA = empty tree
        // Create first commit on main
        store
            .create_revision(
                "org/m",
                Some("4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3"),
                "main_sha",
                "main",
                "main commit",
            )
            .unwrap();

        // Create second commit on main (linear chain)
        store
            .create_revision(
                "org/m",
                Some("main_sha"),
                "second_sha",
                "main",
                "second commit",
            )
            .unwrap();

        // Create third commit on main from second
        store
            .create_revision(
                "org/m",
                Some("second_sha"),
                "third_sha",
                "main",
                "third commit",
            )
            .unwrap();

        let revs = store.list_revisions("org/m").unwrap();
        assert_eq!(revs.len(), 4); // 1 initial + 3 commits

        // HEAD should be at third_sha
        let repo = store.get_repo("org/m").unwrap().unwrap();
        assert_eq!(repo.default_branch, "third_sha");
    }

    #[test]
    fn optimistic_concurrency_rejects_stale_parent() {
        let (_ts, store) = make_store();
        store
            .create_repo(HubRepoType::Model, "org/m", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // First commit succeeds
        store
            .create_revision("org/m", Some(initial_sha), "sha1", "main", "first")
            .unwrap();

        // Second commit with stale parent (initial_sha) should fail
        let result =
            store.create_revision("org/m", Some(initial_sha), "sha_stale", "main", "stale");
        assert!(result.is_err());
    }

    #[test]
    fn repo_types_parse_correctly() {
        assert_eq!(HubRepoType::parse_str("model"), Some(HubRepoType::Model));
        assert_eq!(HubRepoType::parse_str("models"), Some(HubRepoType::Model));
        assert_eq!(
            HubRepoType::parse_str("dataset"),
            Some(HubRepoType::Dataset)
        );
        assert_eq!(
            HubRepoType::parse_str("datasets"),
            Some(HubRepoType::Dataset)
        );
        assert_eq!(HubRepoType::parse_str("space"), Some(HubRepoType::Space));
        assert_eq!(HubRepoType::parse_str("spaces"), Some(HubRepoType::Space));
        assert_eq!(HubRepoType::parse_str("invalid"), None);
    }

    #[test]
    fn hub_repo_type_as_str_roundtrip() {
        let types = [HubRepoType::Model, HubRepoType::Dataset, HubRepoType::Space];
        for rt in &types {
            let s = rt.as_str();
            let parsed = HubRepoType::parse_str(s).unwrap();
            assert_eq!(*rt, parsed);
        }
    }

    #[test]
    fn compute_commit_sha_is_deterministic() {
        let sha1 = HubRepo::compute_commit_sha("parent", "message", "hash").unwrap();
        let sha2 = HubRepo::compute_commit_sha("parent", "message", "hash").unwrap();
        assert_eq!(sha1, sha2);

        let sha3 = HubRepo::compute_commit_sha("parent", "different", "hash").unwrap();
        assert_ne!(sha1, sha3);
    }
}
