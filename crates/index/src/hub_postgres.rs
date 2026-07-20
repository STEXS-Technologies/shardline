use futures_util::TryStreamExt;
use sqlx::Row;

use shardline_protocol::SecretString;

use crate::{
    hub::{
        HubFileEntry, HubRef, HubRepo, HubRepoType, HubRevision, HubStore, HubWebhook,
        canonical_ref_name,
    },
    postgres::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64},
};

const fn repo_type_to_str(t: HubRepoType) -> &'static str {
    t.as_str()
}

fn repo_type_from_str(s: &str) -> Result<HubRepoType, PostgresMetadataStoreError> {
    HubRepoType::parse_str(s).ok_or(PostgresMetadataStoreError::InvalidRepoType(s.to_owned()))
}

/// Escapes LIKE wildcards in user-supplied values to prevent pattern injection.
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('_', "\\_")
        .replace('%', "\\%")
}

/// Runs an async future to completion on the current tokio runtime.
///
/// Uses `block_in_place` to safely transition off the tokio worker thread,
/// then `block_on` to drive the future. This is necessary because `HubStore`
/// trait methods are synchronous but sqlx operations require async.
fn block_on_async<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
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

        block_on_async(async {
            let mut tx = pool.begin().await?;

            sqlx::query(
                "INSERT INTO shardline_hub_repos (repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds)
                 VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM now())::bigint, EXTRACT(EPOCH FROM now())::bigint)
                 ON CONFLICT (repo_id) DO NOTHING",
            )
            .bind(&name)
            .bind(repo_type_str)
            .bind(private)
            .bind(&initial_sha)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                 VALUES ($1, 'main', $2, NULL, NULL, EXTRACT(EPOCH FROM now())::bigint)
                 ON CONFLICT (repo_id, sha) DO NOTHING",
            )
            .bind(&name)
            .bind(&initial_sha)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO shardline_hub_refs (repo_id, ref_name, sha) VALUES ($1, 'main', $2)
                 ON CONFLICT (repo_id, ref_name) DO NOTHING",
            )
            .bind(&name)
            .bind(&initial_sha)
            .execute(&mut *tx)
            .await?;

            let row = sqlx::query(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                 FROM shardline_hub_repos WHERE repo_id = $1",
            )
            .bind(&name)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;

            Ok(HubRepo {
                repo_id: row.try_get("repo_id")?,
                repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?)?,
                private: row.try_get::<bool, _>("private")?,
                default_branch: row.try_get("default_branch")?,
                created_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("created_at_unix_seconds")?,
                )?,
                updated_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("updated_at_unix_seconds")?,
                )?,
            })
        })
    }

    fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let row = sqlx::query(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
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
                repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?)?,
                private: row.try_get::<bool, _>("private")?,
                default_branch: row.try_get("default_branch")?,
                created_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("created_at_unix_seconds")?,
                )?,
                updated_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("updated_at_unix_seconds")?,
                )?,
            }))
        })
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
        let pool = self.pool().clone();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                 FROM shardline_hub_repos ORDER BY repo_id",
            )
            .fetch(&pool);

            let mut repos = Vec::new();
            while let Some(row) = rows.try_next().await? {
                repos.push(HubRepo {
                    repo_id: row.try_get("repo_id")?,
                    repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?)?,
                    private: row.try_get::<bool, _>("private")?,
                    default_branch: row.try_get("default_branch")?,
                    created_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("created_at_unix_seconds")?,
                    )?,
                    updated_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("updated_at_unix_seconds")?,
                    )?,
                });
            }
            Ok(repos)
        })
    }

    fn search_repos(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<HubRepo>, Self::Error> {
        let pool = self.pool().clone();
        let pattern = format!("{}%", escape_like(name_prefix));
        let limit = limit as i64;

        block_on_async(async {
            let mut rows = repo_type.map_or_else(
                || {
                    sqlx::query(
                        "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                         FROM shardline_hub_repos
                         WHERE repo_id LIKE $1
                         ORDER BY repo_id LIMIT $2",
                    )
                    .bind(&pattern)
                    .bind(limit)
                    .fetch(&pool)
                },
                |rt| {
                    let rt_str = rt.as_str();
                    sqlx::query(
                        "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                         FROM shardline_hub_repos
                         WHERE repo_id LIKE $1 AND repo_type = $2
                         ORDER BY repo_id LIMIT $3",
                    )
                    .bind(&pattern)
                    .bind(rt_str)
                    .bind(limit)
                    .fetch(&pool)
                },
            );

            let mut repos = Vec::new();
            while let Some(row) = rows.try_next().await? {
                repos.push(HubRepo {
                    repo_id: row.try_get("repo_id")?,
                    repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?)?,
                    private: row.try_get::<bool, _>("private")?,
                    default_branch: row.try_get("default_branch")?,
                    created_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("created_at_unix_seconds")?,
                    )?,
                    updated_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("updated_at_unix_seconds")?,
                    )?,
                });
            }
            Ok(repos)
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
        let ref_name = canonical_ref_name(ref_name).to_owned();
        let message = message.to_owned();
        let parent_sha = parent_sha.map(ToOwned::to_owned);

        block_on_async(async {
            let mut tx = pool.begin().await?;

            // Optimistic concurrency check
            if let Some(ref parent) = parent_sha {
                let current_ref: Option<String> = sqlx::query_scalar::<_, String>(
                    "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2",
                )
                .bind(&repo_id)
                .bind(&ref_name)
                .fetch_optional(&mut *tx)
                .await?;

                match current_ref {
                    Some(ref current) if current != parent => {
                        return Err(PostgresMetadataStoreError::RecordNotFound);
                    }
                    None => {
                        let parent_exists: bool = sqlx::query_scalar(
                            "SELECT EXISTS(SELECT 1 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2)",
                        )
                        .bind(&repo_id)
                        .bind(parent)
                        .fetch_one(&mut *tx)
                        .await?;
                        if !parent_exists {
                            return Err(PostgresMetadataStoreError::RecordNotFound);
                        }
                    }
                    _ => {}
                }
            }

            if ref_name == "main" {
                sqlx::query(
                    "UPDATE shardline_hub_repos
                     SET default_branch = $1, updated_at_unix_seconds = EXTRACT(EPOCH FROM now())::bigint
                     WHERE repo_id = $2",
                )
                .bind(&new_sha)
                .bind(&repo_id)
                .execute(&mut *tx)
                .await?;
            }

            sqlx::query(
                "INSERT INTO shardline_hub_revisions (repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds)
                 VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM now())::bigint)",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .bind(&new_sha)
            .bind(parent_sha.as_deref())
            .bind(&message)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO shardline_hub_refs (repo_id, ref_name, sha) VALUES ($1, $2, $3)
                 ON CONFLICT (repo_id, ref_name) DO UPDATE SET sha = EXCLUDED.sha",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .bind(&new_sha)
            .execute(&mut *tx)
            .await?;

            let row = sqlx::query(
                "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2",
            )
            .bind(&repo_id)
            .bind(&new_sha)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;

            Ok(HubRevision {
                repo_id: row.try_get("repo_id")?,
                ref_name: row.try_get("ref_name")?,
                sha: row.try_get("sha")?,
                parent_sha: row.try_get("parent_sha")?,
                message: row.try_get("message")?,
                created_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("created_at_unix_seconds")?,
                )?,
            })
        })
    }

    fn list_refs(&self, repo_id: &str) -> Result<Vec<HubRef>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT repo_id, ref_name, sha FROM shardline_hub_refs WHERE repo_id = $1 ORDER BY ref_name",
            )
            .bind(&repo_id)
            .fetch(&pool);
            let mut refs = Vec::new();
            while let Some(row) = rows.try_next().await? {
                refs.push(HubRef {
                    repo_id: row.try_get("repo_id")?,
                    ref_name: row.try_get("ref_name")?,
                    sha: row.try_get("sha")?,
                });
            }
            Ok(refs)
        })
    }

    fn delete_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        expected_sha: &str,
    ) -> Result<(), Self::Error> {
        let repo_id = repo_id.to_owned();
        let ref_name = canonical_ref_name(ref_name).to_owned();
        let expected_sha = expected_sha.to_owned();
        if ref_name == "main" || ref_name == "HEAD" {
            return Err(PostgresMetadataStoreError::RecordNotFound);
        }
        let pool = self.pool().clone();

        block_on_async(async {
            let result = sqlx::query(
                "DELETE FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2 AND sha = $3",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .bind(&expected_sha)
            .execute(&pool)
            .await?;
            if result.rows_affected() != 1 {
                return Err(PostgresMetadataStoreError::RecordNotFound);
            }
            Ok(())
        })
    }

    fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
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
                    created_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("created_at_unix_seconds")?,
                    )?,
                });
            }
            Ok(revisions)
        })
    }

    fn resolve_revision(
        &self,
        repo_id: &str,
        revision: &str,
    ) -> Result<Option<String>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let revision = revision.to_owned();

        block_on_async(async {
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

            let ref_name = canonical_ref_name(&revision);
            let sha: Option<String> = sqlx::query_scalar::<_, String>(
                "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2",
            )
            .bind(&repo_id)
            .bind(ref_name)
            .fetch_optional(&pool)
            .await?;

            Ok(sha)
        })
    }

    fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();
        let files = files.to_vec();

        block_on_async(async {
            let mut tx = pool.begin().await?;
            for file in &files {
                sqlx::query(
                    "INSERT INTO shardline_hub_file_entries (commit_sha, path, size, sha, is_lfs, inline_content)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (commit_sha, path)
                     DO UPDATE SET size = EXCLUDED.size, sha = EXCLUDED.sha, is_lfs = EXCLUDED.is_lfs, inline_content = EXCLUDED.inline_content",
                )
                .bind(&commit_sha)
                .bind(&file.path)
                .bind(u64_to_i64(file.size)?)
                .bind(&file.sha)
                .bind(file.is_lfs)
                .bind(&file.inline_content)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT path, size, sha, is_lfs, inline_content FROM shardline_hub_file_entries
                 WHERE commit_sha = $1 ORDER BY path LIMIT 100000",
            )
            .bind(&commit_sha)
            .fetch(&pool);

            let mut entries = Vec::new();
            while let Some(row) = rows.try_next().await? {
                entries.push(HubFileEntry {
                    path: row.try_get("path")?,
                    size: i64_to_u64(row.try_get::<i64, _>("size")?)?,
                    sha: row.try_get("sha")?,
                    is_lfs: row.try_get::<bool, _>("is_lfs")?,
                    inline_content: row.try_get("inline_content")?,
                });
            }
            Ok(entries)
        })
    }

    fn delete_repo(&self, repo_id: &str) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let mut tx = pool.begin().await?;

            // Delete file entries for all revisions in this repo
            sqlx::query(
                "DELETE FROM shardline_hub_file_entries WHERE commit_sha IN (SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1)",
            )
            .bind(&repo_id)
            .execute(&mut *tx)
            .await?;

            // Delete revisions
            sqlx::query("DELETE FROM shardline_hub_refs WHERE repo_id = $1")
                .bind(&repo_id)
                .execute(&mut *tx)
                .await?;

            sqlx::query("DELETE FROM shardline_hub_revisions WHERE repo_id = $1")
                .bind(&repo_id)
                .execute(&mut *tx)
                .await?;

            // Delete webhooks (explicit, beyond ON DELETE CASCADE)
            sqlx::query("DELETE FROM shardline_hub_webhooks WHERE repo_id = $1")
                .bind(&repo_id)
                .execute(&mut *tx)
                .await?;

            // Delete the repo itself
            sqlx::query("DELETE FROM shardline_hub_repos WHERE repo_id = $1")
                .bind(&repo_id)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            Ok(())
        })
    }

    fn create_webhook(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
    ) -> Result<HubWebhook, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let url = url.to_owned();
        let events_str = events.join(",");
        let secret = secret.map(SecretString::from_secret);
        let events_vec = events.to_vec();

        block_on_async(async {
            let row = sqlx::query(
                "INSERT INTO shardline_hub_webhooks (id, repo_id, url, events, secret, active, created_at_unix_seconds)
                 VALUES (CONCAT('wh-', gen_random_uuid()::text), $1, $2, $3, $4, TRUE, EXTRACT(EPOCH FROM now())::bigint)
                 RETURNING id, repo_id, url, events, secret, active, created_at_unix_seconds",
            )
            .bind(&repo_id)
            .bind(&url)
            .bind(&events_str)
            .bind(secret.as_ref().map(SecretString::as_ref))
            .fetch_one(&pool)
            .await?;

            Ok(HubWebhook {
                id: row.try_get("id")?,
                repo_id: row.try_get("repo_id")?,
                url: row.try_get("url")?,
                events: events_vec,
                secret: row.try_get::<Option<String>, _>("secret")?.map(SecretString::new),
                active: row.try_get::<bool, _>("active")?,
                created_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("created_at_unix_seconds")?,
                )?,
            })
        })
    }

    fn list_webhooks(&self, repo_id: &str) -> Result<Vec<HubWebhook>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT id, repo_id, url, events, secret, active, created_at_unix_seconds
                 FROM shardline_hub_webhooks WHERE repo_id = $1",
            )
            .bind(&repo_id)
            .fetch(&pool);

            let mut webhooks = Vec::new();
            while let Some(row) = rows.try_next().await? {
                let events_str: String = row.try_get("events")?;
                webhooks.push(HubWebhook {
                    id: row.try_get("id")?,
                    repo_id: row.try_get("repo_id")?,
                    url: row.try_get("url")?,
                    events: events_str.split(',').map(ToOwned::to_owned).collect(),
                    secret: row.try_get::<Option<String>, _>("secret")?.map(SecretString::new),
                    active: row.try_get::<bool, _>("active")?,
                    created_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("created_at_unix_seconds")?,
                    )?,
                });
            }
            Ok(webhooks)
        })
    }

    fn delete_webhook(&self, repo_id: &str, webhook_id: &str) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let webhook_id = webhook_id.to_owned();

        block_on_async(async {
            sqlx::query("DELETE FROM shardline_hub_webhooks WHERE repo_id = $1 AND id = $2")
                .bind(&repo_id)
                .bind(&webhook_id)
                .execute(&pool)
                .await?;
            Ok(())
        })
    }

    fn webhooks_for_event(
        &self,
        repo_id: &str,
        event: &str,
    ) -> Result<Vec<HubWebhook>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let event = event.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT id, repo_id, url, events, secret, active, created_at_unix_seconds
                 FROM shardline_hub_webhooks
                 WHERE repo_id = $1 AND active = true AND (',' || events || ',') LIKE ('%,' || $2 || ',%')",
            )
            .bind(&repo_id)
            .bind(&event)
            .fetch(&pool);

            let mut webhooks = Vec::new();
            while let Some(row) = rows.try_next().await? {
                let events_str: String = row.try_get("events")?;
                webhooks.push(HubWebhook {
                    id: row.try_get("id")?,
                    repo_id: row.try_get("repo_id")?,
                    url: row.try_get("url")?,
                    events: events_str.split(',').map(ToOwned::to_owned).collect(),
                    secret: row.try_get::<Option<String>, _>("secret")?.map(SecretString::new),
                    active: row.try_get::<bool, _>("active")?,
                    created_at_unix_seconds: i64_to_u64(
                        row.try_get::<i64, _>("created_at_unix_seconds")?,
                    )?,
                });
            }
            Ok(webhooks)
        })
    }
}

#[cfg(test)]
mod tests {
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::unwrap_in_result, clippy::arithmetic_side_effects, clippy::option_if_let_else, clippy::unreachable, clippy::shadow_unrelated, clippy::let_underscore_must_use)]
    use super::*;
    use crate::hub::{BoxedHubStore, HubRepoType, HubStore};
    use sqlx::postgres::{PgPool, PgPoolOptions};

    // ------------------------------------------------------------------
    // Pure helper function tests (no database needed)
    // ------------------------------------------------------------------
    #[test]
    fn repo_type_to_str_maps_all_variants() {
        assert_eq!(repo_type_to_str(HubRepoType::Model), "model");
        assert_eq!(repo_type_to_str(HubRepoType::Dataset), "dataset");
        assert_eq!(repo_type_to_str(HubRepoType::Space), "space");
    }

    #[test]
    fn repo_type_from_str_parses_all_variants() {
        assert_eq!(repo_type_from_str("model").unwrap(), HubRepoType::Model);
        assert_eq!(repo_type_from_str("dataset").unwrap(), HubRepoType::Dataset);
        assert_eq!(repo_type_from_str("space").unwrap(), HubRepoType::Space);
    }

    #[test]
    fn repo_type_from_str_rejects_unknown() {
        let err = repo_type_from_str("unknown").unwrap_err();
        assert!(matches!(
            err,
            PostgresMetadataStoreError::InvalidRepoType(_)
        ));
        assert_eq!(err.to_string(), "invalid repository type: unknown");
    }

    #[test]
    fn escape_like_preserves_plain_strings() {
        assert_eq!(escape_like("hello"), "hello");
        assert_eq!(escape_like(""), "");
        assert_eq!(escape_like("abc123"), "abc123");
    }

    #[test]
    fn escape_like_escapes_wildcards() {
        assert_eq!(escape_like("foo_bar"), "foo\\_bar");
        assert_eq!(escape_like("foo%bar"), "foo\\%bar");
        assert_eq!(escape_like("foo\\bar"), "foo\\\\bar");
    }

    #[test]
    fn escape_like_escapes_combined_patterns() {
        assert_eq!(escape_like("a%b_c\\d"), "a\\%b\\_c\\\\d");
    }

    // block_on_async itself is tested indirectly by every hub_postgres integration
    // test that exercises the HubStore impl (create_repo, get_repo, etc.)

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
        if let Err(e) = sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha IN (SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1)")
            .bind(repo_id)
            .execute(store.pool())
            .await
        {
            eprintln!("cleanup: failed to delete file entries for {repo_id}: {e}");
        }
        if let Err(e) = sqlx::query("DELETE FROM shardline_hub_refs WHERE repo_id = $1")
            .bind(repo_id)
            .execute(store.pool())
            .await
        {
            eprintln!("cleanup: failed to delete refs for {repo_id}: {e}");
        }
        if let Err(e) = sqlx::query("DELETE FROM shardline_hub_revisions WHERE repo_id = $1")
            .bind(repo_id)
            .execute(store.pool())
            .await
        {
            eprintln!("cleanup: failed to delete revisions for {repo_id}: {e}");
        }
        if let Err(e) = sqlx::query("DELETE FROM shardline_hub_repos WHERE repo_id = $1")
            .bind(repo_id)
            .execute(store.pool())
            .await
        {
            eprintln!("cleanup: failed to delete repo {repo_id}: {e}");
        }
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
        let result = store
            .get_repo("pg-definitely-nonexistent")
            .expect("get_repo");
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

        store
            .create_repo(HubRepoType::Model, "pg-rev-resolve", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        let revs = store.list_revisions("pg-rev-resolve").unwrap();
        assert_eq!(revs.len(), 1);

        let rev = store
            .create_revision(
                "pg-rev-resolve",
                Some(initial_sha),
                "rev2",
                "main",
                "second commit",
            )
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
    async fn pg_delete_ref_preserves_commit_history_and_rejects_stale_delete() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        let repo_id = "pg-delete-ref";
        cleanup_repo(&store, repo_id).await;

        store
            .create_repo(HubRepoType::Model, repo_id, false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        store
            .create_revision(
                repo_id,
                Some(initial_sha),
                "feature-sha",
                "feature",
                "feature commit",
            )
            .unwrap();

        assert!(store.delete_ref(repo_id, "feature", "stale-sha").is_err());
        assert_eq!(
            store
                .resolve_revision(repo_id, "feature")
                .unwrap()
                .as_deref(),
            Some("feature-sha")
        );

        store
            .delete_ref(repo_id, "refs/heads/feature", "feature-sha")
            .unwrap();
        assert_eq!(store.resolve_revision(repo_id, "feature").unwrap(), None);
        assert_eq!(
            store
                .resolve_revision(repo_id, "feature-sha")
                .unwrap()
                .as_deref(),
            Some("feature-sha"),
            "deleting a ref must not remove the commit"
        );
        assert!(store.delete_ref(repo_id, "main", initial_sha).is_err());

        cleanup_repo(&store, repo_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_store_and_get_files() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

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
        ];

        store
            .store_files("pg-commit-files", &files)
            .expect("store_files");
        let retrieved = store.get_files("pg-commit-files").expect("get_files");

        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].path, "a.txt");
        assert!(!retrieved[0].is_lfs);
        assert_eq!(retrieved[1].path, "b.bin");
        assert!(retrieved[1].is_lfs);

        if let Err(e) = sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha = $1")
            .bind("pg-commit-files")
            .execute(store.pool())
            .await
        {
            eprintln!("cleanup: failed to delete file entries for pg-commit-files: {e}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_optimistic_concurrency_rejects_stale_parent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        cleanup_repo(&store, "pg-concurrency").await;

        store
            .create_repo(HubRepoType::Model, "pg-concurrency", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        store
            .create_revision("pg-concurrency", Some(initial_sha), "sha1", "main", "first")
            .unwrap();

        let result = store.create_revision(
            "pg-concurrency",
            Some(initial_sha),
            "sha_stale",
            "main",
            "stale",
        );
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
            path: "test.py".into(),
            size: 42,
            sha: "sha_py".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let commit_sha = sha.unwrap();
        boxed.store_files(&commit_sha, &files).expect("store_files");
        let retrieved = boxed.get_files(&commit_sha).expect("get_files");
        assert_eq!(retrieved.len(), 1);
    }
}
