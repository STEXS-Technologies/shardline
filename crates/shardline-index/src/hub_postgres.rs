use crate::store::escape_like;

mod creation;
mod file_write;
mod object_location;
mod recovery_outbox;
mod ref_batch;

use futures_util::TryStreamExt;
use serde_json::from_value;
use sqlx::{Row, query_scalar};

use shardline_protocol::SecretString;

use crate::{
    hub::{
        EMPTY_HUB_REVISION, HUB_TREE_READ_CEILING, HubFileEntry, HubRef, HubRefBatchOutcome,
        HubRefChange, HubRefCreateOutcome, HubRefPublicationMode, HubRefUpdateOutcome, HubRepo,
        HubRepoSearchOptions, HubRepoType, HubRevision, HubStore, HubTreePage, HubTreePageEntry,
        HubTreePageOptions, HubWebhook, HubWebhookCreateOutcome, canonical_ref_name,
        hub_tree_requires_recovery,
    },
    postgres::{
        PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64,
        load_postgres_latest_evidence_heads, load_postgres_snapshot_evidence_for, u64_to_i64,
    },
};
use shardline_reliability::{
    HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, OperationKind, SnapshotEvidence,
    verify_and_append_snapshot_transition, verify_or_repair_snapshot_evidence,
    verify_snapshot_evidence,
};

fn repo_type_from_str(s: &str) -> Result<HubRepoType, PostgresMetadataStoreError> {
    HubRepoType::parse_str(s).ok_or(PostgresMetadataStoreError::InvalidRepoType(s.to_owned()))
}

fn decode_repo(row: &sqlx::postgres::PgRow) -> Result<HubRepo, PostgresMetadataStoreError> {
    Ok(HubRepo {
        repo_id: row.try_get("repo_id")?,
        repo_type: repo_type_from_str(&row.try_get::<String, _>("repo_type")?)?,
        private: row.try_get::<bool, _>("private")?,
        default_branch: row.try_get("default_branch")?,
        created_at_unix_seconds: i64_to_u64(row.try_get::<i64, _>("created_at_unix_seconds")?)?,
        updated_at_unix_seconds: i64_to_u64(row.try_get::<i64, _>("updated_at_unix_seconds")?)?,
    })
}

fn decode_revision(row: &sqlx::postgres::PgRow) -> Result<HubRevision, PostgresMetadataStoreError> {
    Ok(HubRevision {
        repo_id: row.try_get("repo_id")?,
        ref_name: row.try_get("ref_name")?,
        sha: row.try_get("sha")?,
        parent_sha: row.try_get("parent_sha")?,
        message: row.try_get("message")?,
        created_at_unix_seconds: i64_to_u64(row.try_get::<i64, _>("created_at_unix_seconds")?)?,
    })
}

fn decode_webhook(row: &sqlx::postgres::PgRow) -> Result<HubWebhook, PostgresMetadataStoreError> {
    let events_str: String = row.try_get("events")?;
    Ok(HubWebhook {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        url: row.try_get("url")?,
        events: events_str.split(',').map(ToOwned::to_owned).collect(),
        secret: row
            .try_get::<Option<String>, _>("secret")?
            .map(SecretString::new),
        active: row.try_get::<bool, _>("active")?,
        created_at_unix_seconds: i64_to_u64(row.try_get::<i64, _>("created_at_unix_seconds")?)?,
    })
}

async fn current_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<HubRefEvidenceLog, PostgresMetadataStoreError> {
    let snapshot = HubRefSnapshot::new(repository, ref_name, head_sha)?;
    let evidence = load_postgres_snapshot_evidence_for(&mut **transaction, &snapshot).await?;
    Ok(verify_or_repair_snapshot_evidence(evidence, snapshot)?.0)
}

async fn verify_hub_ref_evidence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = HubRefSnapshot::new(repository, ref_name, head_sha)?;
    let evidence = load_postgres_snapshot_evidence_for(&mut **transaction, &snapshot).await?;
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
        crate::postgres::insert_reliability_event(transaction, event).await?;
    }
    Ok(())
}

async fn verify_hub_ref_evidence_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    refs: &[HubRef],
) -> Result<(), PostgresMetadataStoreError> {
    if refs.is_empty() {
        return Ok(());
    }
    let mut operation_ids = Vec::with_capacity(refs.len());
    for reference in refs {
        operation_ids.push(
            HubRefSnapshot::new(&reference.repo_id, &reference.ref_name, None)?
                .evidence_operation()?
                .operation_id,
        );
    }
    let mut heads = load_postgres_latest_evidence_heads(
        &mut **transaction,
        OperationKind::MetadataCommit,
        &operation_ids,
    )
    .await?;
    for (reference, operation_id) in refs.iter().zip(operation_ids) {
        let snapshot = HubRefSnapshot::new(
            &reference.repo_id,
            &reference.ref_name,
            Some(reference.sha.clone()),
        )?;
        let event_json =
            heads
                .remove(&operation_id)
                .ok_or(PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                ))?;
        let event = from_value::<HubRefLifecycleEvent>(event_json)?;
        let evidence = HubRefEvidenceLog::from_head(event)?;
        verify_snapshot_evidence(&evidence, &snapshot)?;
    }
    Ok(())
}

async fn verify_hub_repo_heads(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repos: &[HubRepo],
) -> Result<(), PostgresMetadataStoreError> {
    let refs = repos
        .iter()
        .map(|repo| HubRef {
            repo_id: repo.repo_id.clone(),
            ref_name: "main".to_owned(),
            sha: repo.default_branch.clone(),
        })
        .collect::<Vec<_>>();
    verify_hub_ref_evidence_batch(transaction, &refs).await
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
    fn object_location(
        &self,
        scope: &shardline_protocol::RepositoryScope,
        kind: crate::hub::HubObjectKind,
        object_id: &str,
    ) -> Result<Option<crate::hub::HubObjectLocation>, Self::Error> {
        object_location::lookup(self, scope, kind, object_id)
    }
    fn remember_object_location(
        &self,
        scope: &shardline_protocol::RepositoryScope,
        kind: crate::hub::HubObjectKind,
        object_id: &str,
    ) -> Result<(), Self::Error> {
        object_location::remember(self, scope, kind, object_id)
    }
    fn lookup_xet_file_identity(
        &self,
        scope: &shardline_protocol::RepositoryScope,
        sha256: &str,
    ) -> Result<Option<crate::XetFileIdentity>, Self::Error> {
        block_on_async(
            crate::PostgresRecordStore::new(self.pool().clone()).xet_file_identity(scope, sha256),
        )
    }
    fn supports_atomic_ref_updates(&self) -> bool {
        true
    }
    fn change_refs_atomically(
        &self,
        repo_id: &str,
        changes: &[crate::hub::HubRefChange],
    ) -> Result<crate::hub::HubRefBatchOutcome, Self::Error> {
        ref_batch::apply(self, repo_id, changes)
    }
    fn change_refs_atomically_with_manifests(
        &self,
        repo_id: &str,
        changes: &[HubRefChange],
        manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    ) -> Result<crate::hub::HubRefBatchOutcome, Self::Error> {
        ref_batch::apply_with_manifests(self, repo_id, changes, manifests)
    }
    fn change_refs_atomically_with_prepared_revisions(
        &self,
        repo_id: &str,
        changes: &[HubRefChange],
        revisions: &[HubRevision],
        manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    ) -> Result<crate::hub::HubRefBatchOutcome, Self::Error> {
        ref_batch::apply_with_prepared_revisions(self, repo_id, changes, revisions, manifests)
    }
    type Error = PostgresMetadataStoreError;

    fn create_repo(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
    ) -> Result<HubRepo, Self::Error> {
        creation::create(self, repo_type, name, private, false, None)
    }

    fn create_repo_authorized(
        &self,
        repo_type: HubRepoType,
        name: &str,
        private: bool,
        actor: Option<&crate::hub::HubCreationActor>,
    ) -> Result<crate::hub::HubCreationOutcome, Self::Error> {
        use crate::hub::HubCreationOutcome;
        match creation::create(self, repo_type, name, private, true, actor) {
            Ok(repo) => Ok(HubCreationOutcome::Created(repo)),
            Err(PostgresMetadataStoreError::Sqlx(error)) => {
                match error.as_database_error().and_then(|e| e.code()).as_deref() {
                    Some("SL403") => Ok(HubCreationOutcome::Denied),
                    Some("SL409") => Ok(HubCreationOutcome::QuotaExceeded),
                    Some("SL429") => Ok(HubCreationOutcome::RateLimited),
                    _ => Err(PostgresMetadataStoreError::Sqlx(error)),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            // Compare the head and its evidence from one MVCC snapshot. A
            // concurrent publication must not look like damaged evidence.
            let mut tx = pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await?;
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

            let repository = decode_repo(&row)?;
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
                    repos.push(decode_repo(&row)?);
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
        self.search_repos_with_options(
            repo_type,
            name_prefix,
            limit,
            &HubRepoSearchOptions::default(),
        )
    }

    fn search_repos_with_options(
        &self,
        repo_type: Option<HubRepoType>,
        name_prefix: &str,
        limit: usize,
        options: &HubRepoSearchOptions,
    ) -> Result<Vec<HubRepo>, Self::Error> {
        let pool = self.pool().clone();
        let pattern = format!("{}%", escape_like(name_prefix));
        let options = options.clone();
        block_on_async(async {
            let mut tx = pool.begin().await?;
            let upper = crate::hub::prefix_successor(name_prefix);
            let upper_predicate = if upper.is_some() {
                "repo_id ~<~ $7"
            } else {
                "$7::TEXT IS NULL"
            };
            let type_predicate = if repo_type.is_some() {
                "repo_type = $2"
            } else {
                "$2::TEXT IS NULL"
            };
            let sql = format!(
                "SELECT repo_id, repo_type, private, default_branch, created_at_unix_seconds, updated_at_unix_seconds
                 FROM shardline_hub_repos
                 WHERE repo_id LIKE $1 ESCAPE '\\'
                   AND repo_id ~>=~ $6 AND {upper_predicate}
                   AND {type_predicate}
                   AND ($3::TEXT IS NULL OR NOT private OR repo_id = $3)
                   AND ($4::TEXT IS NULL OR left(repo_id, length($4) + 1) = $4 || '/')
                 ORDER BY {} LIMIT $5", options.order.sql(),
            );
            // Only fixed SQL fragments and placeholder numbers are assembled; values stay bound.
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(pattern)
                .bind(repo_type.map(HubRepoType::as_str))
                .bind(options.caller_repo_id)
                .bind(options.author)
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .bind(name_prefix)
                .bind(upper)
                .fetch_all(&mut *tx)
                .await?;
            let mut repos = Vec::with_capacity(rows.len());
            for row in rows {
                repos.push(decode_repo(&row)?);
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
        match self.publish_hub_revision(
            repo_id,
            parent_sha,
            new_sha,
            ref_name,
            message,
            HubRefPublicationMode::Upsert,
        )? {
            HubRefCreateOutcome::Created(revision) => Ok(revision),
            HubRefCreateOutcome::AlreadyExists | HubRefCreateOutcome::Unsupported => {
                Err(PostgresMetadataStoreError::RecordNotFound)
            }
        }
    }
    fn publish_prepared_revision(
        &self,
        revision: &HubRevision,
    ) -> Result<HubRefUpdateOutcome, Self::Error> {
        self.publish_prepared_revision_with_manifests(revision, &[])
    }
    fn publish_prepared_revision_with_manifests(
        &self,
        revision: &HubRevision,
        manifests: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    ) -> Result<HubRefUpdateOutcome, Self::Error> {
        self.publish_hub_revision_at(
            &revision.repo_id,
            revision.parent_sha.as_deref(),
            &revision.sha,
            &revision.ref_name,
            revision.message.as_deref().unwrap_or(""),
            HubRefPublicationMode::UpdateOnly,
            Some(revision),
            manifests,
        )
        .map(HubRefCreateOutcome::into_update)
    }

    fn update_revision_if_current(
        &self,
        repo_id: &str,
        expected_sha: &str,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRefUpdateOutcome, Self::Error> {
        self.publish_hub_revision(
            repo_id,
            Some(expected_sha),
            new_sha,
            ref_name,
            message,
            HubRefPublicationMode::UpdateOnly,
        )
        .map(HubRefCreateOutcome::into_update)
    }

    fn create_revision_if_absent(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
    ) -> Result<HubRefCreateOutcome, Self::Error> {
        self.publish_hub_revision(
            repo_id,
            parent_sha,
            new_sha,
            ref_name,
            message,
            HubRefPublicationMode::CreateOnly,
        )
    }

    fn list_refs(&self, repo_id: &str) -> Result<Vec<HubRef>, Self::Error> {
        Ok(self
            .list_refs_bounded(repo_id, usize::MAX)?
            .unwrap_or_default())
    }

    fn list_refs_bounded(
        &self,
        repo_id: &str,
        limit: usize,
    ) -> Result<Option<Vec<HubRef>>, Self::Error> {
        let query_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let mut tx = pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await?;
            let refs = {
                let mut rows = sqlx::query(
                    "SELECT repo_id, ref_name, sha FROM shardline_hub_refs WHERE repo_id = $1 ORDER BY ref_name LIMIT $2",
                )
                .bind(&repo_id)
                .bind(query_limit)
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
            if refs.len() > limit {
                return Ok(None);
            }
            verify_hub_ref_evidence_batch(&mut tx, &refs).await?;
            tx.commit().await?;
            Ok(Some(refs))
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
        let change = HubRefChange {
            ref_name,
            expected_sha: Some(expected_sha),
            new_sha: None,
            message: String::new(),
        };
        match ref_batch::apply(self, &repo_id, &[change])? {
            HubRefBatchOutcome::Applied => Ok(()),
            HubRefBatchOutcome::Conflict => Err(PostgresMetadataStoreError::RecordNotFound),
            HubRefBatchOutcome::Unsupported => {
                Err(PostgresMetadataStoreError::ReliabilityEventConflict(
                    "atomic ref deletion is unsupported".to_owned(),
                ))
            }
        }
    }

    fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
        Ok(self
            .list_revisions_bounded(repo_id, usize::MAX)?
            .unwrap_or_default())
    }

    fn get_revision(&self, repo_id: &str, sha: &str) -> Result<Option<HubRevision>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let sha = sha.to_owned();
        block_on_async(async {
            let row = sqlx::query(
                "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2",
            )
            .bind(&repo_id)
            .bind(&sha)
            .fetch_optional(&pool)
            .await?;
            row.as_ref().map(decode_revision).transpose()
        })
    }

    fn list_revisions_bounded(
        &self,
        repo_id: &str,
        limit: usize,
    ) -> Result<Option<Vec<HubRevision>>, Self::Error> {
        let query_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                 FROM shardline_hub_revisions WHERE repo_id = $1
                 ORDER BY created_at_unix_seconds DESC LIMIT $2",
            )
            .bind(&repo_id)
            .bind(query_limit)
            .fetch(&pool);

            let mut revisions = Vec::new();
            while let Some(row) = rows.try_next().await? {
                revisions.push(decode_revision(&row)?);
            }
            Ok((revisions.len() <= limit).then_some(revisions))
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
            let mut tx = pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await?;
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
        if hub_tree_requires_recovery(commit_sha)
            || (commit_sha == EMPTY_HUB_REVISION && !files.is_empty())
        {
            return Err(PostgresMetadataStoreError::HubTreeRecoveryRequired(
                commit_sha.to_owned(),
            ));
        }
        if commit_sha == EMPTY_HUB_REVISION || files.is_empty() {
            return Ok(());
        }
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();
        let files = files.to_vec();

        block_on_async(async {
            let mut tx = pool.begin().await?;
            file_write::store(&mut tx, &commit_sha, &files).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
        self.get_files_bounded(commit_sha, HUB_TREE_READ_CEILING)?
            .ok_or_else(|| {
                PostgresMetadataStoreError::Unsupported(
                    "Hub tree exceeds the 100000-entry metadata read ceiling".to_owned(),
                )
            })
    }

    fn get_files_bounded(
        &self,
        commit_sha: &str,
        limit: usize,
    ) -> Result<Option<Vec<HubFileEntry>>, Self::Error> {
        let query_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        if commit_sha == EMPTY_HUB_REVISION {
            return Ok(Some(Vec::new()));
        }
        if hub_tree_requires_recovery(commit_sha) {
            return Err(PostgresMetadataStoreError::HubTreeRecoveryRequired(
                commit_sha.to_owned(),
            ));
        }
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();

        block_on_async(async {
            let mut rows = sqlx::query(
                "SELECT path, size, sha, is_lfs FROM shardline_hub_file_entries
                 WHERE commit_sha = $1 ORDER BY path LIMIT $2",
            )
            .bind(&commit_sha)
            .bind(query_limit)
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
            Ok((entries.len() <= limit).then_some(entries))
        })
    }

    fn get_tree_page(
        &self,
        commit_sha: &str,
        options: &HubTreePageOptions,
    ) -> Result<Option<HubTreePage>, Self::Error> {
        if commit_sha == EMPTY_HUB_REVISION {
            return Ok(Some(HubTreePage::from_entries(
                Vec::new(),
                options.page_limit(),
            )));
        }
        if hub_tree_requires_recovery(commit_sha) {
            return Err(PostgresMetadataStoreError::HubTreeRecoveryRequired(
                commit_sha.to_owned(),
            ));
        }
        let pool = self.pool().clone();
        let commit_sha = commit_sha.to_owned();
        let options = options.clone();
        block_on_async(async {
            let mut transaction = pool.begin().await?;
            // A single tree snapshot covers the ceiling, cursor existence and page.
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *transaction)
                .await?;
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM shardline_hub_file_entries WHERE commit_sha=$1 LIMIT 100001) AS bounded")
                .bind(&commit_sha).fetch_one(&mut *transaction).await?;
            if count > i64::try_from(HUB_TREE_READ_CEILING).unwrap_or(i64::MAX) {
                return Ok(None);
            }
            // PostgreSQL text cannot bind NUL. These are unknown paths/cursors
            // for PostgreSQL trees; preserve the prior empty listing only AFTER
            // the complete-tree ceiling and revision guards have been checked.
            if options.path.contains('\0')
                || options
                    .cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor.contains('\0'))
            {
                return Ok(Some(HubTreePage::from_entries(
                    Vec::new(),
                    options.page_limit(),
                )));
            }
            let prefix = options.prefix();
            let upper = (!options.path.is_empty()).then(|| format!("{}0", options.path));
            let query_limit =
                i64::try_from(options.page_limit().saturating_add(1)).unwrap_or(i64::MAX);
            let mut entries = Vec::new();
            let prefix_start =
                i32::try_from(prefix.chars().count().saturating_add(1)).unwrap_or(i32::MAX);
            let sql = if options.recursive {
                "WITH filtered AS (
                       SELECT substring(path FROM $4) COLLATE \"C\" AS relative,size,sha,is_lfs
                       FROM shardline_hub_file_entries WHERE commit_sha=$1 AND path COLLATE \"C\">=$2
                       AND ($3::text IS NULL OR path COLLATE \"C\"<$3) AND path COLLATE \"C\"<>$2),
                     directories AS (
                       SELECT DISTINCT array_to_string(parts[1:level],'/') COLLATE \"C\" AS path
                       FROM (SELECT string_to_array(relative,'/') AS parts FROM filtered) split
                       CROSS JOIN LATERAL generate_series(1,cardinality(parts)-1) level),
                     entries AS (
                       SELECT 0 AS kind,path,NULL::bigint AS size,NULL::text AS sha,NULL::boolean AS is_lfs FROM directories
                       UNION ALL SELECT 1,relative,size,sha,is_lfs FROM filtered)
                     SELECT kind,path,size,sha,is_lfs FROM entries
                     WHERE $5::text IS NULL OR (path>$5 AND EXISTS(SELECT 1 FROM entries WHERE path=$5))
                     ORDER BY path LIMIT $6"
            } else {
                "WITH filtered AS (SELECT substring(path FROM $4) AS relative,size,sha,is_lfs
                       FROM shardline_hub_file_entries WHERE commit_sha=$1 AND path COLLATE \"C\">=$2
                       AND ($3::text IS NULL OR path COLLATE \"C\"<$3)),
                     entries AS (SELECT DISTINCT 0 AS kind,split_part(relative,'/',1) COLLATE \"C\" AS path,
                       NULL::bigint AS size,NULL::text AS sha,NULL::boolean AS is_lfs FROM filtered WHERE strpos(relative,'/')>0
                       UNION ALL SELECT 1,relative COLLATE \"C\",size,sha,is_lfs FROM filtered WHERE strpos(relative,'/')=0),
                     cursor_kind AS (SELECT min(kind) AS kind FROM entries WHERE path=$5)
                     SELECT kind,path,size,sha,is_lfs FROM entries
                     WHERE $5::text IS NULL OR kind>(SELECT kind FROM cursor_kind)
                       OR (kind=(SELECT kind FROM cursor_kind) AND path>$5)
                     ORDER BY kind,path LIMIT $6"
            };
            {
                let mut rows = sqlx::query(sql)
                    .bind(&commit_sha)
                    .bind(&prefix)
                    .bind(&upper)
                    .bind(prefix_start)
                    .bind(&options.cursor)
                    .bind(query_limit)
                    .fetch(&mut *transaction);
                while let Some(row) = rows.try_next().await? {
                    let path: String = row.try_get("path")?;
                    entries.push(if row.try_get::<i32, _>("kind")? == 0 {
                        HubTreePageEntry::Directory { path }
                    } else {
                        HubTreePageEntry::File(HubFileEntry {
                            path,
                            size: i64_to_u64(row.try_get("size")?)?,
                            sha: row.try_get("sha")?,
                            is_lfs: row.try_get("is_lfs")?,
                        })
                    });
                }
            }
            transaction.commit().await?;
            Ok(Some(HubTreePage::from_entries(
                entries,
                options.page_limit(),
            )))
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
                "DELETE FROM shardline_hub_file_entries WHERE commit_sha IN (SELECT sha FROM shardline_hub_revisions WHERE repo_id = $1) AND LENGTH(commit_sha) <> 16 AND NOT EXISTS (SELECT 1 FROM shardline_hub_revisions other WHERE other.sha = shardline_hub_file_entries.commit_sha AND other.repo_id <> $1)",
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

    fn create_webhook_bounded(
        &self,
        repo_id: &str,
        url: &str,
        events: &[String],
        secret: Option<&str>,
        limit: usize,
    ) -> Result<HubWebhookCreateOutcome, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let url = url.to_owned();
        let events = events.to_vec();
        let secret = secret.map(SecretString::from_secret);
        let query_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        block_on_async(async {
            let mut tx = pool.begin().await?;
            let repo_exists: Option<String> = sqlx::query_scalar(
                "SELECT repo_id FROM shardline_hub_repos WHERE repo_id = $1 FOR UPDATE",
            )
            .bind(&repo_id)
            .fetch_optional(&mut *tx)
            .await?;
            if repo_exists.is_none() {
                return Err(PostgresMetadataStoreError::RecordNotFound);
            }
            let duplicate: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM shardline_hub_webhooks WHERE repo_id = $1 AND url = $2)",
            ).bind(&repo_id).bind(&url).fetch_one(&mut *tx).await?;
            if duplicate {
                return Ok(HubWebhookCreateOutcome::Duplicate);
            }
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM shardline_hub_webhooks WHERE repo_id = $1 LIMIT $2) AS bounded",
            ).bind(&repo_id).bind(query_limit).fetch_one(&mut *tx).await?;
            if i64_to_u64(count)? >= u64::try_from(limit).unwrap_or(u64::MAX) {
                return Ok(HubWebhookCreateOutcome::LimitReached);
            }
            let row = sqlx::query(
                "INSERT INTO shardline_hub_webhooks (id, repo_id, url, events, secret, active, created_at_unix_seconds)
                 VALUES (CONCAT('wh-', gen_random_uuid()::text), $1, $2, $3, $4, TRUE, EXTRACT(EPOCH FROM now())::bigint)
                 RETURNING id, repo_id, url, events, secret, active, created_at_unix_seconds",
            ).bind(&repo_id).bind(&url).bind(events.join(","))
                .bind(secret.as_ref().map(SecretString::as_ref)).fetch_one(&mut *tx).await?;
            let webhook = HubWebhook {
                id: row.try_get("id")?,
                repo_id: row.try_get("repo_id")?,
                url: row.try_get("url")?,
                events,
                secret: row
                    .try_get::<Option<String>, _>("secret")?
                    .map(SecretString::new),
                active: row.try_get("active")?,
                created_at_unix_seconds: i64_to_u64(
                    row.try_get::<i64, _>("created_at_unix_seconds")?,
                )?,
            };
            tx.commit().await?;
            Ok(HubWebhookCreateOutcome::Created(webhook))
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
                webhooks.push(decode_webhook(&row)?);
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

    fn compare_and_swap_webhook_secret(
        &self,
        repo_id: &str,
        webhook_id: &str,
        expected_secret: &str,
        secret: Option<&str>,
    ) -> Result<Option<bool>, Self::Error> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let webhook_id = webhook_id.to_owned();
        let expected_secret = SecretString::from_secret(expected_secret);
        let secret = secret.map(SecretString::from_secret);
        block_on_async(async {
            let changed = sqlx::query(
                "UPDATE shardline_hub_webhooks SET secret=$1 WHERE repo_id=$2 AND id=$3 AND secret COLLATE pg_catalog.\"C\"=$4",
            ).bind(secret.as_ref().map(SecretString::as_ref)).bind(&repo_id).bind(&webhook_id)
                .bind(expected_secret.as_ref()).execute(&pool).await?.rows_affected();
            Ok(Some(changed == 1))
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
                webhooks.push(decode_webhook(&row)?);
            }
            Ok(webhooks)
        })
    }
}

impl PostgresIndexStore {
    fn publish_hub_revision(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
        mode: HubRefPublicationMode,
    ) -> Result<HubRefCreateOutcome, PostgresMetadataStoreError> {
        self.publish_hub_revision_at(
            repo_id,
            parent_sha,
            new_sha,
            ref_name,
            message,
            mode,
            None,
            &[],
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "shared transactional legacy and prepared publication"
    )]
    fn publish_hub_revision_at(
        &self,
        repo_id: &str,
        parent_sha: Option<&str>,
        new_sha: &str,
        ref_name: &str,
        message: &str,
        mode: HubRefPublicationMode,
        prepared: Option<&HubRevision>,
        manifest_refs: &[shardline_reliability::HubRefBatchManifestReferenceV1],
    ) -> Result<HubRefCreateOutcome, PostgresMetadataStoreError> {
        let pool = self.pool().clone();
        let repo_id = repo_id.to_owned();
        let new_sha = new_sha.to_owned();
        let ref_name = canonical_ref_name(ref_name).to_owned();
        let message = message.to_owned();
        let parent_sha = parent_sha.map(ToOwned::to_owned);
        let prepared_timestamp = prepared
            .map(|r| u64_to_i64(r.created_at_unix_seconds))
            .transpose()?;
        let manifest_refs = manifest_refs.to_vec();

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

            if mode.conflicts_with(current_ref.as_deref(), parent_sha.as_deref()) {
                return Ok(HubRefCreateOutcome::AlreadyExists);
            }

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
                 VALUES ($1, $2, $3, $4, $5, COALESCE($6, EXTRACT(EPOCH FROM now())::bigint))
                 ON CONFLICT (repo_id, sha) DO NOTHING",
            )
            .bind(&repo_id)
            .bind(&ref_name)
            .bind(&new_sha)
            .bind(parent_sha.as_deref())
            .bind(&message)
            .bind(prepared_timestamp)
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
            recovery_outbox::append_ref_batch(
                &mut tx,
                &repo_id,
                &[HubRefChange {
                    ref_name: ref_name.clone(),
                    expected_sha: parent_sha.clone(),
                    new_sha: Some(new_sha.clone()),
                    message: message.clone(),
                }],
                &manifest_refs,
            )
            .await?;

            let row = sqlx::query(
                "SELECT repo_id, ref_name, sha, parent_sha, message, created_at_unix_seconds
                 FROM shardline_hub_revisions WHERE repo_id = $1 AND sha = $2",
            )
            .bind(&repo_id)
            .bind(&new_sha)
            .fetch_one(&mut *tx)
            .await?;

            let revision = decode_revision(&row)?;
            if prepared.is_some_and(|expected| !revision.matches_prepared(expected)) {
                return Err(PostgresMetadataStoreError::RecordNotFound);
            }
            tx.commit().await?;
            Ok(HubRefCreateOutcome::Created(revision))
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
    use crate::postgres::connect_isolated_postgres as connect_postgres;
    use sqlx::postgres::PgPool;

    // ------------------------------------------------------------------
    // Pure helper function tests (no database needed)
    // ------------------------------------------------------------------
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

    // block_on_async itself is tested indirectly by every hub_postgres integration
    // test that exercises the HubStore impl (create_repo, get_repo, etc.)

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atomic_ref_batch_contract() {
        if std::env::var_os("DATABASE_URL").is_none() {
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool);
        crate::hub::ref_batch_tests::contract(&store);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_shadow_outbox_is_domain_ordered() {
        use crate::hub::{HubRefBatchOutcome, HubRefChange};
        use crate::{HubRecoveryMode, HubRecoveryOutboxStore, put_hub_ref_archive_record};
        use ed25519_dalek::SigningKey;
        use shardline_storage::LocalObjectStore;
        if std::env::var_os("DATABASE_URL").is_none() {
            eprintln!("skipping Postgres outbox test: no DATABASE_URL");
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool);
        let repo = "recovery/pg-order";
        store.create_repo(HubRepoType::Model, repo, false).unwrap();
        store
            .create_revision(repo, None, "base", "main", "Initial")
            .unwrap();
        store
            .create_revision(repo, Some("base"), "stable", "stable", "Stable")
            .unwrap();
        store
            .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some("pg-deployment"))
            .await
            .unwrap();
        assert_eq!(
            store.change_refs_atomically(repo, &[]).unwrap(),
            HubRefBatchOutcome::Applied
        );
        let no_op_changes = [
            HubRefChange {
                ref_name: "main".into(),
                expected_sha: Some("base".into()),
                new_sha: Some("base".into()),
                message: "No-op main".into(),
            },
            HubRefChange {
                ref_name: "stable".into(),
                expected_sha: Some("stable".into()),
                new_sha: Some("stable".into()),
                message: "No-op stable".into(),
            },
        ];
        assert_eq!(
            store.change_refs_atomically(repo, &no_op_changes).unwrap(),
            HubRefBatchOutcome::Applied
        );
        let no_op_outbox_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_hub_recovery_outbox WHERE repo_id = $1",
        )
        .bind(repo)
        .fetch_one(store.pool())
        .await
        .unwrap();
        let no_op_sequence_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_hub_recovery_sequences WHERE repo_id = $1",
        )
        .bind(repo)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!((no_op_outbox_count, no_op_sequence_count), (0, 0));
        for (expected_sha, new_sha) in [("base", "one"), ("one", "two")] {
            assert_eq!(
                store
                    .change_refs_atomically(
                        repo,
                        &[HubRefChange {
                            ref_name: "main".into(),
                            expected_sha: Some(expected_sha.into()),
                            new_sha: Some(new_sha.into()),
                            message: new_sha.into(),
                        }],
                    )
                    .unwrap(),
                HubRefBatchOutcome::Applied
            );
        }
        let first = store
            .next_pending_hub_ref_event(repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.domain_sequence, 1);
        let archive_root = shardline_test_support::TempStorage::new();
        let archive = LocalObjectStore::new(archive_root.path().join("archive")).unwrap();
        let signing_key = SigningKey::from_bytes(&[31; 32]);
        let archived_first =
            put_hub_ref_archive_record(&archive, first.event.clone(), &signing_key, "pg-test-key")
                .await
                .unwrap();
        let mut out_of_order = first.clone();
        out_of_order.domain_sequence = 2;
        assert!(
            store
                .mark_hub_ref_event_archived(&out_of_order, &archived_first)
                .await
                .is_err()
        );
        store
            .mark_hub_ref_event_archived(&first, &archived_first)
            .await
            .unwrap();
        let second = store
            .next_pending_hub_ref_event(repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.domain_sequence, 2);
        assert_eq!(second.event.after.refs[0].head_sha.as_deref(), Some("two"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_prepared_manifest_reference_is_atomic() {
        use crate::{HubRecoveryMode, HubRecoveryOutboxStore};
        use shardline_reliability::HubRefBatchManifestReferenceV1;
        if std::env::var_os("DATABASE_URL").is_none() {
            eprintln!("skipping PostgreSQL prepared-manifest test: no DATABASE_URL");
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool);
        let repo = "recovery/pg-prepared-manifest";
        store.create_repo(HubRepoType::Model, repo, false).unwrap();
        store
            .create_revision(repo, None, "base", "main", "Initial")
            .unwrap();
        let prepared = HubRevision {
            repo_id: repo.to_owned(),
            ref_name: "main".to_owned(),
            sha: "b".repeat(40),
            parent_sha: Some("base".to_owned()),
            message: Some("prepared".to_owned()),
            created_at_unix_seconds: 1234,
        };
        let digest = "a".repeat(64);
        let manifest = HubRefBatchManifestReferenceV1::new(
            "pg-manifest-deployment".into(),
            repo.into(),
            prepared.sha.clone(),
            digest.clone(),
            format!(
                "recovery/v1/{}/{}/versions/aa/{digest}",
                crate::hub_recovery::stable_id_segment("pg-manifest-deployment"),
                crate::hub_recovery::stable_id_segment(repo)
            ),
        )
        .unwrap();
        assert!(
            store
                .publish_prepared_revision_with_manifests(
                    &prepared,
                    std::slice::from_ref(&manifest)
                )
                .is_err()
        );
        let head: String = sqlx::query_scalar(
            "SELECT sha FROM shardline_hub_refs WHERE repo_id = $1 AND ref_name = 'main'",
        )
        .bind(repo)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(head, "base");
        store
            .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some("pg-manifest-deployment"))
            .await
            .unwrap();
        assert!(matches!(
            store
                .publish_prepared_revision_with_manifests(
                    &prepared,
                    std::slice::from_ref(&manifest)
                )
                .unwrap(),
            HubRefUpdateOutcome::Updated(_)
        ));
        let revision = store.get_revision(repo, &prepared.sha).unwrap().unwrap();
        assert!(revision.matches_prepared(&prepared));
        let item = store
            .next_pending_hub_ref_event(repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.event.after.manifest_refs, vec![manifest]);
        assert_eq!(
            item.event.after.refs[0].head_sha.as_deref(),
            Some(prepared.sha.as_str())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_prepared_ref_batch_preserves_timestamp_and_duplicate_sha_heads() {
        use crate::hub::{HubRefBatchOutcome, HubRefChange};
        use crate::{HubRecoveryMode, HubRecoveryOutboxStore};
        use shardline_reliability::HubRefBatchManifestReferenceV1;
        if std::env::var_os("DATABASE_URL").is_none() {
            eprintln!("skipping PostgreSQL prepared ref batch test: no DATABASE_URL");
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool);
        let repo = "recovery/pg-prepared-ref-batch";
        let deployment = "pg-prepared-ref-batch-deployment";
        store.create_repo(HubRepoType::Model, repo, false).unwrap();
        store
            .create_revision(repo, None, "base", "main", "Initial")
            .unwrap();
        store
            .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some(deployment))
            .await
            .unwrap();

        let sha = "e".repeat(40);
        let prepared = HubRevision {
            repo_id: repo.to_owned(),
            ref_name: "main".to_owned(),
            sha: sha.clone(),
            parent_sha: Some("base".to_owned()),
            message: Some("prepared batch".to_owned()),
            created_at_unix_seconds: 1234,
        };
        let digest = "f".repeat(64);
        let manifest = HubRefBatchManifestReferenceV1::new(
            deployment.to_owned(),
            repo.to_owned(),
            sha.clone(),
            digest.clone(),
            format!(
                "recovery/v1/{}/{}/versions/ff/{digest}",
                crate::hub_recovery::stable_id_segment(deployment),
                crate::hub_recovery::stable_id_segment(repo),
            ),
        )
        .unwrap();
        let changes = [
            HubRefChange {
                ref_name: "main".into(),
                expected_sha: Some("base".into()),
                new_sha: Some(sha.clone()),
                message: "prepared batch".into(),
            },
            HubRefChange {
                ref_name: "refs/heads/feature".into(),
                expected_sha: None,
                new_sha: Some(sha.clone()),
                message: "prepared batch".into(),
            },
        ];
        assert!(
            store
                .change_refs_atomically_with_prepared_revisions(
                    repo,
                    &changes,
                    std::slice::from_ref(&prepared),
                    &[],
                )
                .is_err()
        );
        let extra_sha = "f".repeat(40);
        let extra_digest = "a".repeat(64);
        let extra_manifest = HubRefBatchManifestReferenceV1::new(
            deployment.to_owned(),
            repo.to_owned(),
            extra_sha,
            extra_digest.clone(),
            format!(
                "recovery/v1/{}/{}/versions/aa/{extra_digest}",
                crate::hub_recovery::stable_id_segment(deployment),
                crate::hub_recovery::stable_id_segment(repo),
            ),
        )
        .unwrap();
        assert!(
            store
                .change_refs_atomically_with_prepared_revisions(
                    repo,
                    &changes,
                    std::slice::from_ref(&prepared),
                    &[manifest.clone(), extra_manifest],
                )
                .is_err()
        );
        let sequence_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_hub_recovery_sequences WHERE repo_id = $1)",
        )
        .bind(repo)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert!(!sequence_exists);
        assert_eq!(
            store
                .change_refs_atomically_with_prepared_revisions(
                    repo,
                    &changes,
                    std::slice::from_ref(&prepared),
                    std::slice::from_ref(&manifest),
                )
                .unwrap(),
            HubRefBatchOutcome::Applied
        );
        let revision = store.get_revision(repo, &sha).unwrap().unwrap();
        assert!(revision.matches_prepared(&prepared));
        assert_eq!(store.list_refs(repo).unwrap().len(), 2);
        let item = store
            .next_pending_hub_ref_event(repo)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.event.after.refs.len(), 2);
        assert_eq!(item.event.after.manifest_refs, [manifest]);
        let source = store.hub_ref_checkpoint_source(repo).await.unwrap();
        assert_eq!(source.deployment_id.as_deref(), Some(deployment));
        assert_eq!(source.committed_sequence, 1);
        assert_eq!(source.archived_sequence, 0);
        assert_eq!(source.refs.len(), 2);
        assert_eq!(
            store
                .hub_repositories_after(None, std::num::NonZeroUsize::new(1).unwrap())
                .await
                .unwrap(),
            vec![repo.to_owned()]
        );

        let conflicting = HubRevision {
            created_at_unix_seconds: prepared.created_at_unix_seconds + 1,
            ..prepared
        };
        assert!(
            store
                .change_refs_atomically_with_prepared_revisions(
                    repo,
                    &[HubRefChange {
                        ref_name: "stable".into(),
                        expected_sha: None,
                        new_sha: Some(sha.clone()),
                        message: "prepared batch".into(),
                    }],
                    &[conflicting],
                    &[],
                )
                .is_err()
        );
        let sequence: i64 = sqlx::query_scalar(
            "SELECT last_sequence FROM shardline_hub_recovery_sequences WHERE repo_id = $1",
        )
        .bind(repo)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(sequence, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn postgres_outbox_adapter_is_async_runtime_safe() {
        use crate::{HubRecoveryMode, HubRecoveryOutboxStore};
        if std::env::var_os("DATABASE_URL").is_none() {
            eprintln!("skipping Postgres async outbox test: no DATABASE_URL");
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool);
        store
            .set_hub_recovery_mode(HubRecoveryMode::Off, None)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atomic_ref_batch_storage_failure_rolls_back_evidence_and_refs() {
        use crate::hub::{HubRefBatchOutcome, HubRefChange};
        if std::env::var_os("DATABASE_URL").is_none() {
            return;
        }
        let pool = connect_postgres()
            .await
            .expect("configured PostgreSQL fixture must connect");
        let store = make_store(pool.clone());
        let repo = "atomic/failure";
        store.create_repo(HubRepoType::Model, repo, false).unwrap();
        store
            .create_revision(repo, None, "old", "main", "Initial")
            .unwrap();
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE FUNCTION atomic_ref_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ref failure'; END $$;
            CREATE TRIGGER atomic_ref_failure BEFORE INSERT ON shardline_hub_refs FOR EACH ROW WHEN (NEW.ref_name = 'reject') EXECUTE FUNCTION atomic_ref_failure();")
            .execute(&pool).await.unwrap();
        let changes = [
            HubRefChange {
                ref_name: "main".into(),
                expected_sha: Some("old".into()),
                new_sha: Some("next".into()),
                message: "Next".into(),
            },
            HubRefChange {
                ref_name: "reject".into(),
                expected_sha: None,
                new_sha: Some("new".into()),
                message: "New".into(),
            },
        ];
        let result = store.change_refs_atomically(repo, &changes);
        sqlx::raw_sql("DROP TRIGGER atomic_ref_failure ON shardline_hub_refs; DROP FUNCTION atomic_ref_failure();").execute(&pool).await.unwrap();
        assert!(result.is_err());
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(
            store.resolve_revision(repo, "main").unwrap().as_deref(),
            Some("old")
        );
        assert_eq!(store.get_repo(repo).unwrap().unwrap().default_branch, "old");
        assert!(store.get_revision(repo, "next").unwrap().is_none());
        assert!(store.resolve_revision(repo, "reject").unwrap().is_none());
        assert_eq!(
            store.change_refs_atomically(repo, &changes).unwrap(),
            HubRefBatchOutcome::Applied
        );
        assert_eq!(store.list_refs(repo).unwrap().len(), 2);
    }

    fn make_store(pool: PgPool) -> PostgresIndexStore {
        PostgresIndexStore::new(pool)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_create_and_get_repo() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

        let repo = store
            .create_repo(HubRepoType::Model, "pg-create-get", false)
            .expect("create_repo");

        assert_eq!(repo.repo_id, "pg-create-get");
        assert_eq!(repo.repo_type, HubRepoType::Model);
        assert!(!repo.private);

        let fetched = store.get_repo("pg-create-get").expect("get_repo");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().repo_id, "pg-create-get");

        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(
            store
                .create_repo(HubRepoType::Dataset, "pg-create-get", true)
                .is_err()
        );
        let preserved = store.get_repo("pg-create-get").unwrap().unwrap();
        assert_eq!(preserved.repo_type, HubRepoType::Model);
        assert!(!preserved.private);
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(
            before, after,
            "duplicate creation must not append ref evidence"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_concurrent_repository_creators_have_one_winner() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("SKIPPED: PostgreSQL creation race requires DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        let repo = "pg-create-race";
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let mut creators = Vec::new();
        for attempt in 0..8 {
            let store = store.clone();
            let barrier = barrier.clone();
            creators.push(tokio::task::spawn_blocking(move || {
                barrier.wait();
                let private = attempt % 2 == 0;
                (
                    private,
                    store.create_repo(HubRepoType::Model, repo, private),
                )
            }));
        }
        let mut winner = None;
        for creator in creators {
            let (private, result) = creator.await.unwrap();
            if result.is_ok() {
                assert!(winner.replace(private).is_none());
            }
        }
        assert_eq!(
            store.get_repo(repo).unwrap().unwrap().private,
            winner.unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_hub_same_ref_legacy_writer_is_rejected_by_reliability_gate() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool.clone());
        let repo_id = "pg-mixed-version-ref";
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
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_repo_list_and_search_reject_tampered_head_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool.clone());
        let repo_id = "pg-list-search-tampered";

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
        assert!(store.list_refs(repo_id).is_err());
        assert!(store.list_repos().is_err());
        assert!(store.search_repos(None, repo_id, 10).is_err());
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
        let loaded = store
            .get_revision("pg-rev-resolve", "rev2")
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                loaded.repo_id,
                loaded.sha,
                loaded.parent_sha,
                loaded.message,
                loaded.created_at_unix_seconds
            ),
            (
                rev.repo_id,
                rev.sha,
                rev.parent_sha,
                rev.message,
                rev.created_at_unix_seconds
            )
        );
        assert!(
            store
                .get_revision("pg-other-repo", "rev2")
                .unwrap()
                .is_none()
        );

        let sha = store.resolve_revision("pg-rev-resolve", "rev2").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));

        let sha = store.resolve_revision("pg-rev-resolve", "main").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));

        let sha = store.resolve_revision("pg-rev-resolve", "").unwrap();
        assert_eq!(sha.as_deref(), Some("rev2"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_delete_ref_preserves_commit_history_and_rejects_stale_delete() {
        use crate::{HubRecoveryMode, HubRecoveryOutboxStore};

        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);
        let repo_id = "pg-delete-ref";

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
            .set_hub_recovery_mode(HubRecoveryMode::Shadow, Some("delete-test-deployment"))
            .await
            .unwrap();

        store
            .delete_ref(repo_id, "refs/heads/feature", "feature-sha")
            .unwrap();
        let item = store
            .next_pending_hub_ref_event(repo_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.domain_sequence, 1);
        assert_eq!(
            item.event.before.refs[0].head_sha.as_deref(),
            Some("feature-sha")
        );
        assert_eq!(item.event.after.refs[0].head_sha, None);
        assert_eq!(store.resolve_revision(repo_id, "feature").unwrap(), None);
        assert_eq!(
            store
                .resolve_revision(repo_id, "feature-sha")
                .unwrap()
                .as_deref(),
            Some("feature-sha"),
            "deleting a ref must not remove the commit"
        );
        assert!(store.delete_ref(repo_id, "feature", "feature-sha").is_err());
        let last_sequence: i64 = sqlx::query_scalar(
            "SELECT last_sequence FROM shardline_hub_recovery_sequences WHERE repo_id = $1",
        )
        .bind(repo_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            last_sequence, 1,
            "stale delete must not allocate a sequence"
        );
        assert!(store.delete_ref(repo_id, "main", initial_sha).is_err());
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
        let xmin_before: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_hub_file_entries
             WHERE commit_sha = $1 AND path = $2",
        )
        .bind("pg-commit-files")
        .bind("a.txt")
        .fetch_one(store.pool())
        .await
        .expect("read xmin before replay");
        store
            .store_files("pg-commit-files", &files)
            .expect("replay identical files");
        store
            .store_files("pg-commit-files", &[])
            .expect("empty batch preserves the committed file entries");
        let xmin_after: String = sqlx::query_scalar(
            "SELECT xmin::text FROM shardline_hub_file_entries
             WHERE commit_sha = $1 AND path = $2",
        )
        .bind("pg-commit-files")
        .bind("a.txt")
        .fetch_one(store.pool())
        .await
        .expect("read xmin after replay");
        let retrieved = store.get_files("pg-commit-files").expect("get_files");

        assert_eq!(
            xmin_before, xmin_after,
            "identical replay must not update tuple"
        );
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].path, "a.txt");
        assert!(!retrieved[0].is_lfs);
        assert_eq!(retrieved[1].path, "b.bin");
        assert!(retrieved[1].is_lfs);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_optimistic_concurrency_rejects_stale_parent() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_concurrent_ref_updates_have_exactly_one_winner() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let first_store = make_store(pool.clone());
        let second_store = make_store(pool.clone());
        let verifier = make_store(pool);
        let repo_id = "pg-concurrent-ref-cas";
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_delete_and_push_cannot_leave_a_resurrected_ref() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let push_store = make_store(pool.clone());
        let delete_store = make_store(pool.clone());
        let verifier = make_store(pool.clone());
        let repo_id = "pg-delete-push-race";
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
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_boxed_hub_store_e2e() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping Postgres test: no DATABASE_URL");
            return;
        };
        let store = make_store(pool);

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
        // Give file entries their own revision to exercise revision publication.
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_strict_updates_reject_missing_refs_and_deletion_takes_repository_lock() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = make_store(pool.clone());
        let repo = "pg-strict-delete-lock";
        store.create_repo(HubRepoType::Model, repo, true).unwrap();
        store
            .create_revision(repo, Some(EMPTY_HUB_REVISION), "old", "selected", "old")
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let holder: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT repo_id FROM shardline_hub_repos WHERE repo_id=$1 FOR UPDATE")
            .bind(repo)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let deleter = store.clone();
        let delete =
            tokio::task::spawn_blocking(move || deleter.delete_ref(repo, "selected", "old"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))",
            ).bind(holder).fetch_one(&pool).await.unwrap();
            if blocked {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "deletion did not wait on repository lock"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(!delete.is_finished());
        tx.commit().await.unwrap();
        delete.await.unwrap().unwrap();
        let revisions = store.list_revisions(repo).unwrap().len();
        let events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_reliability_events WHERE operation_id LIKE $1",
        )
        .bind(format!("{}:{repo}%", repo.len()))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(matches!(
            store
                .update_revision_if_current(repo, "old", "new", "selected", "restore")
                .unwrap(),
            HubRefUpdateOutcome::Conflict
        ));
        assert_eq!(store.list_revisions(repo).unwrap().len(), revisions);
        assert!(
            !store
                .list_refs(repo)
                .unwrap()
                .iter()
                .any(|r| r.ref_name == "selected")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM shardline_reliability_events WHERE operation_id LIKE $1"
            )
            .bind(format!("{}:{repo}%", repo.len()))
            .fetch_one(&pool)
            .await
            .unwrap(),
            events
        );
        store
            .create_revision(repo, Some("old"), "moved", "selected", "intentional branch")
            .unwrap();
        assert!(matches!(
            store
                .update_revision_if_current(repo, "old", "stale", "selected", "stale")
                .unwrap(),
            HubRefUpdateOutcome::Conflict
        ));
        assert!(matches!(
            store
                .update_revision_if_current(repo, "moved", "new", "refs/heads/selected", "update")
                .unwrap(),
            HubRefUpdateOutcome::Updated(_)
        ));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_atomic_webhook_registration_handles_boundary_and_concurrency() {
        let Some(pool) = connect_postgres().await else {
            assert!(
                std::env::var_os("DATABASE_URL").is_none()
                    && std::env::var_os("SHARDLINE_INDEX_POSTGRES_URL").is_none(),
                "configured PostgreSQL fixture must connect and initialize"
            );
            return;
        };
        let store = make_store(pool);
        crate::hub::assert_atomic_webhook_registration(&store);
        crate::hub::assert_atomic_webhook_secret_upgrade(&store);
        eprintln!("PostgreSQL atomic webhook boundary and concurrency checks executed");
    }
}
