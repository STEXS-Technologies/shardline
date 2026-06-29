use futures_util::TryStreamExt;
use sqlx::Row;

use crate::{
    hub::{HubFileEntry, HubRepo, HubRepoType, HubRevision, HubStore},
    postgres::{PostgresIndexStore, PostgresMetadataStoreError},
};

fn repo_type_to_str(t: HubRepoType) -> &'static str {
    t.as_str()
}

fn repo_type_from_str(s: &str) -> HubRepoType {
    HubRepoType::from_str(s).unwrap_or(HubRepoType::Model)
}

impl HubStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Self::Error> {
        let pool = self.pool().clone();
        let repo_type_str = repo_type_to_str(repo_type);
        let name = name.to_owned();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3".to_owned();

        let _result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "INSERT INTO shardline_hub_repos (repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds)
                     VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM now())::bigint, EXTRACT(EPOCH FROM now())::bigint)
                     ON CONFLICT (repo_id) DO NOTHING",
                )
                .bind(&name)
                .bind(repo_type_str)
                .bind(private)
                .bind(&initial_sha)
                .execute(&pool)
                .await?;

                sqlx::query(
                    "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                     VALUES ($1, 'main', $2, NULL, NULL, EXTRACT(EPOCH FROM now())::bigint)
                     ON CONFLICT (repo_id, sha) DO NOTHING",
                )
                .bind(&name)
                .bind(&initial_sha)
                .execute(&pool)
                .await?;

                Ok::<_, PostgresMetadataStoreError>(())
            })
        })?;

        Ok(HubRepo {
            repo_id: name,
            repo_type,
            private,
            default_branch: initial_sha,
            created_at_unix_seconds: 0,
        })
    }

    fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let row = sqlx::query(
                    "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
                     FROM shardline_hub_repos WHERE repo_id = $1",
                )
                .bind(&repo_id)
                .fetch_optional(&pool)
                .await?;

                let Some(row) = row else {
                    return Ok(None);
                };

                Ok(Some(HubRepo {
                    repo_id: row.try_get("repo_id")?,
                    repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?),
                    private: row.try_get::<bool, _>("private")?,
                    default_branch: row.try_get("default_branch")?,
                    created_at_unix_seconds: row.try_get::<i64, _>("created_at_unix_seconds")? as u64,
                }))
            })
        })
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
        let pool = self.pool().clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut rows = sqlx::query(
                    "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds
                     FROM shardline_hub_repos ORDER BY repo_id",
                )
                .fetch(&pool);

                let mut repos = Vec::new();
                while let Some(row) = rows.try_next().await? {
                    repos.push(HubRepo {
                        repo_id: row.try_get("repo_id")?,
                        repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?),
                        private: row.try_get::<bool, _>("private")?,
                        default_branch: row.try_get("default_branch")?,
                        created_at_unix_seconds: row.try_get::<i64, _>("created_at_unix_seconds")? as u64,
                    });
                }
                Ok(repos)
            })
        })
    }

    fn create_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRevision, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let new_sha = new_sha.to_owned();
        let ref_name = ref_name.to_owned();
        let message = message.to_owned();
        let parent_sha = parent_sha.map(ToOwned::to_owned);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Optimistic concurrency check
                if let Some(ref parent) = parent_sha {
                    let current_head: Option<String> = sqlx::query_scalar::<_, String>(
                        "SELECT default_branch FROM shardline_hub_repos WHERE repo_id = $1",
                    )
                    .bind(&repo_id)
                    .fetch_optional(&pool)
                    .await?;

                    match current_head {
                        Some(ref head) if head != parent => {
                            return Err(PostgresMetadataStoreError::RecordNotFound);
                        }
                        None => {
                            return Err(PostgresMetadataStoreError::RecordNotFound);
                        }
                        _ => {}
                    }
                }

                sqlx::query(
                    "UPDATE shardline_hub_repos
                     SET default_branch = $1, updated_at_unix_seconds = EXTRACT(EPOCH FROM now())::bigint
                     WHERE repo_id = $2",
                )
                .bind(&new_sha)
                .bind(&repo_id)
                .execute(&pool)
                .await?;

                sqlx::query(
                    "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                     VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM now())::bigint)",
                )
                .bind(&repo_id)
                .bind(&ref_name)
                .bind(&new_sha)
                .bind(parent_sha.as_deref())
                .bind(&message)
                .execute(&pool)
                .await?;

                Ok(HubRevision {
                    repo_id,
                    ref_name,
                    sha: new_sha,
                    parent_sha: parent_sha.as_deref().map(ToOwned::to_owned),
                    message: Some(message),
                    created_at_unix_seconds: 0,
                })
            })
        })
    }

    fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut rows = sqlx::query(
                    "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                     FROM shardline_hub_revisions WHERE repo_id = $1
                     ORDER BY created_at_unix_seconds DESC",
                )
                .bind(&repo_id)
                .fetch(&pool);

                let mut revisions = Vec::new();
                while let Some(row) = rows.try_next().await? {
                    revisions.push(HubRevision {
                        repo_id: row.try_get("repo_id")?,
                        ref_name: row.try_get("ref_name")?,
                        sha: row.try_get("sha")?,
                        parent_sha: row.try_get("parent_sha")?,
                        message: row.try_get("message")?,
                        created_at_unix_seconds: row.try_get::<i64, _>("created_at_unix_seconds")? as u64,
                    });
                }
                Ok(revisions)
            })
        })
    }

    fn resolve_revision(&self, repo_id: &str, revision: &str) -> Result<Option<String>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let revision = revision.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if revision.is_empty() || revision == "main" {
                    let head: Option<String> = sqlx::query_scalar::<_, String>(
                        "SELECT default_branch FROM shardline_hub_repos WHERE repo_id = $1",
                    )
                    .bind(&repo_id)
                    .fetch_optional(&pool)
                    .await?;
                    return Ok(head);
                }

                let exists: bool = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2)",
                )
                .bind(&repo_id)
                .bind(&revision)
                .fetch_one(&pool)
                .await?;

                if exists {
                    return Ok(Some(revision));
                }

                let sha: Option<String> = sqlx::query_scalar::<_, String>(
                    "SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1 AND ref_name = $2
                     ORDER BY created_at_unix_seconds DESC LIMIT 1",
                )
                .bind(&repo_id)
                .bind(&revision)
                .fetch_optional(&pool)
                .await?;

                Ok(sha)
            })
        })
    }

    fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();
        let files = files.to_vec();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                for file in &files {
                    sqlx::query(
                        "INSERT INTO shardline_hub_file_entries (commit_sha, path, size, sha, is_lfs)
                         VALUES ($1, $2, $3, $4, $5)
                         ON CONFLICT (commit_sha, path)
                         DO UPDATE SET size = EXCLUDED.size, sha = EXCLUDED.sha, is_lfs = EXCLUDED.is_lfs",
                    )
                    .bind(&commit_sha)
                    .bind(&file.path)
                    .bind(file.size as i64)
                    .bind(&file.sha)
                    .bind(file.is_lfs)
                    .execute(&pool)
                    .await?;
                }
                Ok(())
            })
        })
    }

    fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut rows = sqlx::query(
                    "SELECT path, size, sha, is_lfs FROM shardline_hub_file_entries
                     WHERE commit_sha = $1 ORDER BY path",
                )
                .bind(&commit_sha)
                .fetch(&pool);

                let mut entries = Vec::new();
                while let Some(row) = rows.try_next().await? {
                    entries.push(HubFileEntry {
                        path: row.try_get("path")?,
                        size: row.try_get::<i64, _>("size")? as u64,
                        sha: row.try_get("sha")?,
                        is_lfs: row.try_get::<bool, _>("is_lfs")?,
                    });
                }
                Ok(entries)
            })
        })
    }

    fn put_lfs_object(&self, oid: &str, data: &[u8]) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let oid = oid.to_owned();
        let data = data.to_vec();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "INSERT INTO shardline_hub_lfs_objects (oid, data, size, created_at_unix_seconds)
                     VALUES ($1, $2, $3, EXTRACT(EPOCH FROM now())::bigint)
                     ON CONFLICT (oid) DO UPDATE SET data = EXCLUDED.data, size = EXCLUDED.size",
                )
                .bind(&oid)
                .bind(&data)
                .bind(data.len() as i64)
                .execute(&pool)
                .await?;
                Ok(())
            })
        })
    }

    fn get_lfs_object(&self, oid: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let pool = self.pool().clone();
        let oid = oid.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let row = sqlx::query("SELECT data FROM shardline_hub_lfs_objects WHERE oid = $1")
                    .bind(&oid)
                    .fetch_optional(&pool)
                    .await?;

                let Some(row) = row else {
                    return Ok(None);
                };

                Ok(Some(row.try_get::<Vec<u8>, _>("data")?))
            })
        })
    }

    fn has_lfs_object(&self, oid: &str) -> Result<bool, Self::Error> {
        let pool = self.pool().clone();
        let oid = oid.to_owned();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let exists: bool = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM shardline_hub_lfs_objects WHERE oid = $1)",
                )
                .bind(&oid)
                .fetch_one(&pool)
                .await?;
                Ok(exists)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{BoxedHubStore, HubRepoType, HubStore};
    use sqlx::postgres::{PgPoolOptions, PgPool};

    async fn connect_postgres() -> Option<PgPool> {
        let url = std::env::var("DATABASE_URL")
            .or_else(|_| std::env::var("SHARDLINE_INDEX_POSTGRES_URL"))
            .ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .ok()?;
        Some(pool)
    }

    fn make_store(pool: PgPool) -> PostgresIndexStore {
        PostgresIndexStore::new(pool)
    }

    async fn cleanup_repo(store: &PostgresIndexStore, repo_id: &str) {
        let _ = sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha IN (SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1)")
            .bind(repo_id)
            .execute(store.pool())
            .await;
        let _ = sqlx::query("DELETE FROM shardline_hub_revisions WHERE repo_id = $1")
            .bind(repo_id)
            .execute(store.pool())
            .await;
        let _ = sqlx::query("DELETE FROM shardline_hub_repos WHERE repo_id = $1")
            .bind(repo_id)
            .execute(store.pool())
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_create_and_get_repo() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        cleanup_repo(&store, "pg-create-get").await;

        let repo = store
            .create_repo(HubRepoType::Model, "pg-create-get", false)
            .expect("create_repo");

        assert_eq!(repo.repo_id, "pg-create-get");
        assert_eq!(repo.repo_type, HubRepoType::Model);
        assert!(!repo.private);

        let fetched = store.get_repo("pg-create-get").expect("get_repo");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().repo_id, "pg-create-get");

        cleanup_repo(&store, "pg-create-get").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_get_repo_returns_none_for_missing() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        let result = store.get_repo("pg-definitely-nonexistent").expect("get_repo");
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_create_revision_and_resolve() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        cleanup_repo(&store, "pg-rev-resolve").await;

        store.create_repo(HubRepoType::Model, "pg-rev-resolve", false).unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        let revs = store.list_revisions("pg-rev-resolve").unwrap();
        assert_eq!(revs.len(), 1);

        let rev = store
            .create_revision("pg-rev-resolve", Some(initial_sha), "rev2", "main", "second commit")
            .unwrap();
        assert_eq!(rev.sha, "rev2");

        let sha = store.resolve_revision("pg-rev-resolve", "rev2").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));

        let sha = store.resolve_revision("pg-rev-resolve", "main").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));

        let sha = store.resolve_revision("pg-rev-resolve", "").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));

        cleanup_repo(&store, "pg-rev-resolve").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_store_and_get_files() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

        let files = vec![
            HubFileEntry { path: "a.txt".into(), size: 100, sha: "sha_a".into(), is_lfs: false },
            HubFileEntry { path: "b.bin".into(), size: 2048, sha: "sha_b".into(), is_lfs: true },
        ];

        store.store_files("pg-commit-files", &files).expect("store_files");
        let retrieved = store.get_files("pg-commit-files").expect("get_files");

        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].path, "a.txt");
        assert!(!retrieved[0].is_lfs);
        assert_eq!(retrieved[1].path, "b.bin");
        assert!(retrieved[1].is_lfs);

        let _ = sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha = $1")
            .bind("pg-commit-files")
            .execute(store.pool())
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_put_and_get_lfs_object() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

        store.put_lfs_object("pg-lfs-oid1", b"hello pg lfs").expect("put_lfs_object");

        let retrieved = store.get_lfs_object("pg-lfs-oid1").expect("get_lfs_object");
        assert_eq!(retrieved.as_deref(), Some(b"hello pg lfs" as &[u8]));

        assert!(store.has_lfs_object("pg-lfs-oid1").expect("has_lfs_object"));
        assert!(!store.has_lfs_object("pg-lfs-nope").expect("has_lfs_object"));

        let _ = sqlx::query("DELETE FROM shardline_hub_lfs_objects WHERE oid = $1")
            .bind("pg-lfs-oid1")
            .execute(store.pool())
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_optimistic_concurrency_rejects_stale_parent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        cleanup_repo(&store, "pg-concurrency").await;

        store.create_repo(HubRepoType::Model, "pg-concurrency", false).unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        store
            .create_revision("pg-concurrency", Some(initial_sha), "sha1", "main", "first")
            .unwrap();

        let result = store.create_revision("pg-concurrency", Some(initial_sha), "sha_stale", "main", "stale");
        assert!(result.is_err());

        cleanup_repo(&store, "pg-concurrency").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_boxed_hub_store_e2e() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        cleanup_repo(&store, "pg-boxed").await;

        let boxed = BoxedHubStore::from_store(store);

        let repo = boxed
            .create_repo(HubRepoType::Space, "pg-boxed", true)
            .expect("create_repo via boxed");
        assert_eq!(repo.repo_id, "pg-boxed");
        assert!(repo.private);

        let fetched = boxed.get_repo("pg-boxed").expect("get_repo via boxed");
        assert!(fetched.is_some());

        let repos = boxed.list_repos().expect("list_repos");
        assert!(repos.iter().any(|r| r.repo_id == "pg-boxed"));

        let revs = boxed.list_revisions("pg-boxed").expect("list_revisions");
        assert_eq!(revs.len(), 1);

        let sha = boxed.resolve_revision("pg-boxed", "main").expect("resolve");
        assert!(sha.is_some());

        let files = vec![HubFileEntry {
            path: "test.py".into(), size: 42, sha: "sha_py".into(), is_lfs: false,
        }];
        let commit_sha = sha.unwrap();
        boxed.store_files(&commit_sha, &files).expect("store_files");
        let retrieved = boxed.get_files(&commit_sha).expect("get_files");
        assert_eq!(retrieved.len(), 1);

        boxed.put_lfs_object("pg-boxed-oid", b"boxed data").expect("put_lfs");
        assert!(boxed.has_lfs_object("pg-boxed-oid").unwrap());
        let data = boxed.get_lfs_object("pg-boxed-oid").unwrap().unwrap();
        assert_eq!(&data, b"boxed data");
    }
}
