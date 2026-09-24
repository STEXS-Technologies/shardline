use futures_util::TryStreamExt;
use serde_json::from_value;
use sqlx::{Row, query_scalar};

use shardline_protocol::SecretString;

use crate::{
    hub::{
        HubFileEntry, HubRef, HubRepo, HubRepoType, HubRevision, HubStore, HubWebhook,
        canonical_ref_name,
    },
    postgres::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64},
};
use shardline_reliability::{
    HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, OperationKind, SnapshotEvidence,
    verify_and_append_snapshot_transition, verify_or_repair_snapshot_evidence,
    verify_persisted_event_merkle_chain, verify_snapshot_evidence,
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

async fn load_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository: &str,
    ref_name: &str,
) -> Result<HubRefEvidenceLog, PostgresMetadataStoreError> {
    let operation = HubRefSnapshot::new(repository, ref_name, None)?.evidence_operation()?;
    let rows = sqlx::query(
        "SELECT event_json, merkle_commit_json FROM shardline_reliability_events
         WHERE operation_kind = 'MetadataCommit' AND operation_id = $1 ORDER BY sequence",
    )
    .bind(&operation.operation_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    let mut event_json = Vec::with_capacity(rows.len());
    let mut merkle_commits = Vec::with_capacity(rows.len());
    for row in rows {
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(from_value::<HubRefLifecycleEvent>(value.clone())?);
        event_json.push(value);
        merkle_commits.push(row.try_get("merkle_commit_json")?);
    }
    verify_persisted_event_merkle_chain(
        OperationKind::MetadataCommit,
        &event_json,
        &merkle_commits,
    )?;
    Ok(HubRefEvidenceLog::from_events(events)?)
}

async fn current_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<HubRefEvidenceLog, PostgresMetadataStoreError> {
    let snapshot = HubRefSnapshot::new(repository, ref_name, head_sha)?;
    let evidence = load_hub_ref_evidence(transaction, repository, ref_name).await?;
    Ok(verify_or_repair_snapshot_evidence(evidence, snapshot)?.0)
}

async fn verify_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = HubRefSnapshot::new(repository, ref_name, head_sha)?;
    let evidence = load_hub_ref_evidence(transaction, repository, ref_name).await?;
    verify_snapshot_evidence(&evidence, &snapshot)?;
    Ok(())
}

async fn persist_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    evidence: &HubRefEvidenceLog,
) -> Result<(), PostgresMetadataStoreError> {
    let Some(first) = evidence.events().first() else {
        return Ok(());
    };
    let persisted_sequence: Option<i64> = query_scalar(
        "SELECT MAX(sequence)
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2",
    )
    .bind(first.operation.kind.as_str())
    .bind(&first.operation.operation_id)
    .fetch_one(&mut **transaction)
    .await?;
    let persisted_sequence = persisted_sequence.unwrap_or(-1);
    for event in evidence.events() {
        if u64_to_i64(event.sequence)? <= persisted_sequence {
            continue;
        }
        crate::postgres::insert_reliability_event(&mut **transaction, event).await?;
    }
    Ok(())
}

async fn verify_hub_repo_heads(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repos: &[HubRepo],
) -> Result<(), PostgresMetadataStoreError> {
    for repo in repos {
        verify_hub_ref_evidence(
            transaction,
            &repo.repo_id,
            "main",
            Some(repo.default_branch.clone()),
        )
        .await?;
    }
    Ok(())
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
            let evidence = HubRefEvidenceLog::baseline(HubRefSnapshot::new(
                &name,
                "main",
                Some(initial_sha.clone()),
            )?)?;
            persist_hub_ref_evidence(&mut tx, &evidence).await?;

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
            let mut tx = pool.begin().await?;
            let row = sqlx::query(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                 FROM shardline_hub_repos WHERE repo_id = $1",
            )
            .bind(&repo_id)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(row) = row else {
                tx.commit().await?;
                return Ok(None);
            };

            let repository = HubRepo {
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
            };
            verify_hub_ref_evidence(
                &mut tx,
                &repository.repo_id,
                "main",
                Some(repository.default_branch.clone()),
            )
            .await?;
            tx.commit().await?;
            Ok(Some(repository))
        })
    }

    fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
        let pool = self.pool().clone();

        block_on_async(async {
            let mut tx = pool.begin().await?;
            let mut repos = Vec::new();
            {
                let mut rows = sqlx::query(
                    "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                     FROM shardline_hub_repos ORDER BY repo_id FOR SHARE",
                )
                .fetch(&mut *tx);
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
            }
            verify_hub_repo_heads(&mut tx, &repos).await?;
            tx.commit().await?;
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
            let mut tx = pool.begin().await?;
            let mut repos = Vec::new();
            {
                let mut rows = if let Some(rt) = repo_type {
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
                    .fetch(&mut *tx)
                } else {
                    sqlx::query(
                        "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                         FROM shardline_hub_repos
                         WHERE repo_id LIKE $1
                         ORDER BY repo_id LIMIT $2",
                    )
                    .bind(&pattern)
                    .bind(limit)
                    .fetch(&mut *tx)
                };
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
            }
            verify_hub_repo_heads(&mut tx, &repos).await?;
            tx.commit().await?;
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

            // Serialize every mutable ref operation for this repository across
            // Postgres-backed Shardline replicas. The row lock also excludes
            // `delete_repo`, so a commit cannot be acknowledged into a
            // repository concurrently removed by another process.
            let locked_repo: Option<String> = sqlx::query_scalar(
                "SELECT repo_id FROM shardline_hub_repos WHERE repo_id = $1 FOR UPDATE",
            )
            .bind(&repo_id)
            .fetch_optional(&mut *tx)
            .await?;
            if locked_repo.is_none() {
                return Err(PostgresMetadataStoreError::RecordNotFound);
            }

            let current_ref: Option<String> = sqlx::query_scalar(
                "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .fetch_optional(&mut *tx)
            .await?;

            // Optimistic concurrency check
            if let Some(ref parent) = parent_sha {
                match current_ref.as_deref() {
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

            let before = HubRefSnapshot::new(&repo_id, &ref_name, current_ref.clone())?;
            let after = HubRefSnapshot::new(&repo_id, &ref_name, Some(new_sha.clone()))?;
            let evidence = verify_and_append_snapshot_transition(
                current_hub_ref_evidence(&mut tx, &repo_id, &ref_name, current_ref).await?,
                before,
                after,
            )?
            .0;
            persist_hub_ref_evidence(&mut tx, &evidence).await?;

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
            let mut tx = pool.begin().await?;
            let refs = {
                let mut rows = sqlx::query(
                    "SELECT repo_id, ref_name, sha FROM shardline_hub_refs WHERE repo_id = $1 ORDER BY ref_name",
                )
                .bind(&repo_id)
                .fetch(&mut *tx);
                let mut refs = Vec::new();
                while let Some(row) = rows.try_next().await? {
                    refs.push(HubRef {
                        repo_id: row.try_get("repo_id")?,
                        ref_name: row.try_get("ref_name")?,
                        sha: row.try_get("sha")?,
                    });
                }
                refs
            };
            for reference in &refs {
                verify_hub_ref_evidence(
                    &mut tx,
                    &reference.repo_id,
                    &reference.ref_name,
                    Some(reference.sha.clone()),
                )
                .await?;
            }
            tx.commit().await?;
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
            let mut tx = pool.begin().await?;
            let result = sqlx::query(
                "DELETE FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2 AND sha = $3",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .bind(&expected_sha)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() != 1 {
                return Err(PostgresMetadataStoreError::RecordNotFound);
            }
            let before = HubRefSnapshot::new(&repo_id, &ref_name, Some(expected_sha.clone()))?;
            let after = HubRefSnapshot::new(&repo_id, &ref_name, None)?;
            let evidence = verify_and_append_snapshot_transition(
                current_hub_ref_evidence(&mut tx, &repo_id, &ref_name, Some(expected_sha)).await?,
                before,
                after,
            )?
            .0;
            persist_hub_ref_evidence(&mut tx, &evidence).await?;
            tx.commit().await?;
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
            let mut tx = pool.begin().await?;
            if revision.is_empty() || revision == "main" {
                let head: Option<String> = sqlx::query_scalar::<_, String>(
                    "SELECT default_branch FROM shardline_hub_repos WHERE repo_id = $1",
                )
                .bind(&repo_id)
                .fetch_optional(&mut *tx)
                .await?;
                if let Some(head) = &head {
                    verify_hub_ref_evidence(&mut tx, &repo_id, "main", Some(head.clone())).await?;
                }
                tx.commit().await?;
                return Ok(head);
            }

            let exists: bool = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2)",
            )
            .bind(&repo_id)
            .bind(&revision)
            .fetch_one(&mut *tx)
            .await?;

            if exists {
                tx.commit().await?;
                return Ok(Some(revision));
            }

            let ref_name = canonical_ref_name(&revision);
            let sha: Option<String> = sqlx::query_scalar::<_, String>(
                "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2",
            )
            .bind(&repo_id)
            .bind(ref_name)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(current_sha) = &sha {
                verify_hub_ref_evidence(&mut tx, &repo_id, ref_name, Some(current_sha.clone()))
                    .await?;
            }
            tx.commit().await?;

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
                    "INSERT INTO shardline_hub_file_entries (commit_sha, path, size, sha, is_lfs)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (commit_sha, path)
                     DO UPDATE SET size = EXCLUDED.size, sha = EXCLUDED.sha, is_lfs = EXCLUDED.is_lfs",
                )
                .bind(&commit_sha)
                .bind(&file.path)
                .bind(u64_to_i64(file.size)?)
                .bind(&file.sha)
                .bind(file.is_lfs)
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
                "SELECT path, size, sha, is_lfs FROM shardline_hub_file_entries
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

            // Use the same repository-row lock as `create_revision`. This
            // makes delete-vs-push ordering explicit across replicas.
            let locked_repo: Option<String> = sqlx::query_scalar(
                "SELECT repo_id FROM shardline_hub_repos WHERE repo_id = $1 FOR UPDATE",
            )
            .bind(&repo_id)
            .fetch_optional(&mut *tx)
            .await?;
            if locked_repo.is_none() {
                return Ok(());
            }

            let ref_names: Vec<String> =
                sqlx::query_scalar("SELECT ref_name FROM shardline_hub_refs WHERE repo_id = $1")
                    .bind(&repo_id)
                    .fetch_all(&mut *tx)
                    .await?;
            for ref_name in ref_names {
                let current_sha: String = sqlx::query_scalar(
                    "SELECT sha FROM shardline_hub_refs
                     WHERE repo_id = $1 AND ref_name = $2",
                )
                .bind(&repo_id)
                .bind(&ref_name)
                .fetch_one(&mut *tx)
                .await?;
                let before = HubRefSnapshot::new(&repo_id, &ref_name, Some(current_sha.clone()))?;
                let after = HubRefSnapshot::new(&repo_id, &ref_name, None)?;
                let evidence = verify_and_append_snapshot_transition(
                    current_hub_ref_evidence(&mut tx, &repo_id, &ref_name, Some(current_sha))
                        .await?,
                    before,
                    after,
                )?
                .0;
                persist_hub_ref_evidence(&mut tx, &evidence).await?;
                sqlx::query("DELETE FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = $2")
                    .bind(&repo_id)
                    .bind(&ref_name)
                    .execute(&mut *tx)
                    .await?;
            }

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
                secret: row
                    .try_get::<Option<String>, _>("secret")?
                    .map(SecretString::new),
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
                    secret: row
                        .try_get::<Option<String>, _>("secret")?
                        .map(SecretString::new),
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

    fn update_webhook_secret(
        &self,
        repo_id: &str,
        webhook_id: &str,
        secret: Option<&str>,
    ) -> Result<(), Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let webhook_id = webhook_id.to_owned();
        let secret = secret.map(SecretString::from_secret);

        block_on_async(async {
            sqlx::query(
                "UPDATE shardline_hub_webhooks SET secret = $1 WHERE repo_id = $2 AND id = $3",
            )
            .bind(secret.as_ref().map(SecretString::as_ref))
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
        let event = escape_like(event);

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
                    secret: row
                        .try_get::<Option<String>, _>("secret")?
                        .map(SecretString::new),
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
    use crate::hub::{BoxedHubStore, HubRepoType, HubStore};
    use serial_test::serial;
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
        // Test cleanup must satisfy the same deferred delete gate as a real
        // mutation.  Seed a terminal evidence event for every existing ref,
        // delete the materialized rows in that transaction, then remove the
        // synthetic history after commit.
        let cleanup_result = async {
            let mut tx = store.pool().begin().await?;
            let refs: Vec<(String, String)> = sqlx::query_as(
                "SELECT ref_name, sha FROM shardline_hub_refs WHERE repo_id = $1",
            )
            .bind(repo_id)
            .fetch_all(&mut *tx)
            .await?;
            for (ref_name, _sha) in &refs {
                let operation_id = format!(
                    "{}:{}{}:{}",
                    repo_id.len(),
                    repo_id,
                    ref_name.len(),
                    ref_name
                );
                let sequence: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(sequence), 0) + 1
                     FROM shardline_reliability_events
                     WHERE operation_kind = 'MetadataCommit' AND operation_id = $1",
                )
                .bind(&operation_id)
                .fetch_one(&mut *tx)
                .await?;
                let event_json = serde_json::json!({
                    "after": {
                        "repository": repo_id,
                        "ref_name": ref_name,
                        "head_sha": null
                    }
                });
                sqlx::query(
                    "INSERT INTO shardline_reliability_events
                     (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
                     VALUES ('MetadataCommit', $1, $2, $3, $4)",
                )
                .bind(&operation_id)
                .bind(sequence)
                .bind(event_json)
                .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query("DELETE FROM shardline_hub_file_entries WHERE commit_sha IN (SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1)")
                .bind(repo_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM shardline_hub_refs WHERE repo_id = $1")
                .bind(repo_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM shardline_hub_revisions WHERE repo_id = $1")
                .bind(repo_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM shardline_hub_repos WHERE repo_id = $1")
                .bind(repo_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await
        }
        .await;
        if let Err(e) = cleanup_result {
            eprintln!("cleanup: failed to remove repository {repo_id}: {e}");
        }
        if let Err(e) = sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'MetadataCommit'
               AND operation_id LIKE $1 || '%'",
        )
        .bind(format!("{}:{repo_id}", repo_id.len()))
        .execute(store.pool())
        .await
        {
            eprintln!("cleanup: failed to delete repository evidence for {repo_id}: {e}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(hub_postgres)]
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
    #[serial(hub_postgres)]
    async fn pg_hub_same_ref_legacy_writer_is_rejected_by_reliability_gate() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool.clone());
        let repo_id = "pg-mixed-version-ref";
        cleanup_repo(&store, repo_id).await;
        let original = store
            .create_repo(HubRepoType::Model, repo_id, false)
            .expect("create repo");

        let mut transaction = pool.begin().await.expect("begin legacy transaction");
        sqlx::query(
            "UPDATE shardline_hub_refs
             SET sha = $3
             WHERE repo_id = $1 AND ref_name = $2",
        )
        .bind(repo_id)
        .bind("main")
        .bind("legacy-overwrite")
        .execute(&mut *transaction)
        .await
        .expect("legacy write reaches deferred gate");
        assert!(transaction.commit().await.is_err());
        assert_eq!(
            store.get_repo(repo_id).expect("read after rejected write"),
            Some(original)
        );
        cleanup_repo(&store, repo_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(hub_postgres)]
    async fn pg_repo_list_and_search_reject_tampered_head_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool.clone());
        let repo_id = "pg-list-search-tampered";
        cleanup_repo(&store, repo_id).await;

        store
            .create_repo(HubRepoType::Model, repo_id, false)
            .expect("create_repo");
        let operation_id = HubRefSnapshot::new(repo_id, "main", None)
            .expect("valid hub ref snapshot")
            .evidence_operation()
            .expect("valid hub ref operation")
            .operation_id;
        sqlx::query(
            "UPDATE shardline_reliability_events
             SET event_json = '{}'::jsonb
             WHERE operation_kind = 'MetadataCommit' AND operation_id = $1",
        )
        .bind(&operation_id)
        .execute(&pool)
        .await
        .expect("tamper hub evidence");

        assert!(store.list_repos().is_err());
        assert!(store.search_repos(None, repo_id, 10).is_err());

        sqlx::query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'MetadataCommit' AND operation_id = $1",
        )
        .bind(&operation_id)
        .execute(&pool)
        .await
        .expect("clean up tampered hub evidence");
        cleanup_repo(&store, repo_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(hub_postgres)]
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
    #[serial(hub_postgres)]
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
    #[serial(hub_postgres)]
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
    #[serial(hub_postgres)]
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
            },
            HubFileEntry {
                path: "b.bin".into(),
                size: 2048,
                sha: "sha_b".into(),
                is_lfs: true,
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
    #[serial(hub_postgres)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial(hub_postgres)]
    async fn pg_concurrent_ref_updates_have_exactly_one_winner() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let first_store = make_store(pool.clone());
        let second_store = make_store(pool.clone());
        let verifier = make_store(pool);
        let repo_id = "pg-concurrent-ref-cas";
        cleanup_repo(&first_store, repo_id).await;
        first_store
            .create_repo(HubRepoType::Model, repo_id, false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_store.create_revision(repo_id, Some(initial_sha), "first-sha", "main", "first")
        });
        let second = tokio::spawn(async move {
            barrier.wait().await;
            second_store.create_revision(repo_id, Some(initial_sha), "second-sha", "main", "second")
        });

        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_ne!(first.is_ok(), second.is_ok());

        let resolved = verifier.resolve_revision(repo_id, "main").unwrap().unwrap();
        assert_eq!(
            resolved,
            if first.is_ok() {
                "first-sha"
            } else {
                "second-sha"
            }
        );
        cleanup_repo(&verifier, repo_id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial(hub_postgres)]
    async fn pg_delete_and_push_cannot_leave_a_resurrected_ref() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let push_store = make_store(pool.clone());
        let delete_store = make_store(pool.clone());
        let verifier = make_store(pool.clone());
        let repo_id = "pg-delete-push-race";
        cleanup_repo(&push_store, repo_id).await;
        push_store
            .create_repo(HubRepoType::Model, repo_id, false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let push_barrier = barrier.clone();
        let push = tokio::spawn(async move {
            push_barrier.wait().await;
            push_store.create_revision(
                repo_id,
                Some(initial_sha),
                "racing-sha",
                "main",
                "racing push",
            )
        });
        let delete = tokio::spawn(async move {
            barrier.wait().await;
            delete_store.delete_repo(repo_id)
        });

        let _push_result = push.await.unwrap();
        delete.await.unwrap().unwrap();
        assert!(verifier.get_repo(repo_id).unwrap().is_none());
        assert!(verifier.list_refs(repo_id).unwrap().is_empty());
        assert!(verifier.list_revisions(repo_id).unwrap().is_empty());

        let dangling_files: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shardline_hub_file_entries
             WHERE commit_sha = 'racing-sha'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(dangling_files, 0);
        cleanup_repo(&verifier, repo_id).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(hub_postgres_listing)]
    #[serial(hub_postgres)]
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

        // The initial empty-tree revision is shared by every new repository.
        // Use a repository-specific revision for file entries so this test does
        // not collide with cleanup performed by other Postgres tests.
        let initial_sha = sha.unwrap();
        let file_commit_sha = "pg-boxed-files";
        boxed
            .create_revision(
                "pg-boxed",
                Some(&initial_sha),
                file_commit_sha,
                "files",
                "add test file",
            )
            .expect("create revision for files");

        let files = vec![HubFileEntry {
            path: "test.py".into(),
            size: 42,
            sha: "sha_py".into(),
            is_lfs: false,
        }];
        boxed
            .store_files(file_commit_sha, &files)
            .expect("store_files");
        let retrieved = boxed.get_files(file_commit_sha).expect("get_files");
        assert_eq!(retrieved.len(), 1);

        boxed.delete_repo("pg-boxed").expect("cleanup boxed repo");
    }
}
