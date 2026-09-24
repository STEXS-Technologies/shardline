use std::collections::HashMap;

use serde_json::{from_value, to_value};
use sqlx::{PgConnection, Row, postgres::PgRow, query, query_scalar};

use super::{PostgresIndexStore, PostgresMetadataStoreError, u64_to_i64};
use crate::{OciTagEntry, OciTagStore};
use shardline_reliability::{
    OciTagEvidenceLog, OciTagLifecycleEvent, OciTagSnapshot, OperationKind,
    ReliabilityMerkleCommit, SnapshotEvidence, persisted_event_sequence,
    reliability_merkle_commit_json_with_previous, verify_and_append_snapshot_transition,
    verify_or_repair_snapshot_evidence, verify_persisted_merkle_commit_with_previous,
    verify_snapshot_event, verify_snapshot_evidence,
};

fn entry_from_row(row: &PgRow) -> Result<OciTagEntry, PostgresMetadataStoreError> {
    Ok(OciTagEntry {
        scope_namespace: row.try_get("scope_namespace")?,
        repository: row.try_get("repository")?,
        tag: row.try_get("tag")?,
        digest_hex: row.try_get("digest_hex")?,
    })
}

async fn load_tag_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
) -> Result<OciTagEvidenceLog, PostgresMetadataStoreError> {
    let operation =
        OciTagSnapshot::new(scope_namespace, repository, tag, None)?.evidence_operation()?;
    let event = super::index_store::load_postgres_latest_evidence_event(
        &mut *connection,
        OperationKind::OciTag,
        &operation.operation_id,
    )
    .await?;
    let Some(event) = event else {
        return Ok(OciTagEvidenceLog::default());
    };
    Ok(OciTagEvidenceLog::from_head(from_value(event)?)?)
}

async fn current_tag_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    digest_hex: Option<String>,
) -> Result<OciTagEvidenceLog, PostgresMetadataStoreError> {
    let snapshot = OciTagSnapshot::new(scope_namespace, repository, tag, digest_hex)?;
    let loaded = load_tag_evidence(connection, scope_namespace, repository, tag).await?;
    Ok(verify_or_repair_snapshot_evidence(loaded, snapshot)?.0)
}

async fn verify_tag_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    digest_hex: Option<String>,
) -> Result<(), PostgresMetadataStoreError> {
    let snapshot = OciTagSnapshot::new(scope_namespace, repository, tag, digest_hex)?;
    let evidence = load_tag_evidence(connection, scope_namespace, repository, tag).await?;
    verify_snapshot_evidence(&evidence, &snapshot)?;
    Ok(())
}

async fn verify_tag_listing_evidence(
    connection: &mut PgConnection,
    values: &[OciTagEntry],
) -> Result<(), PostgresMetadataStoreError> {
    if values.is_empty() {
        return Ok(());
    }
    let snapshots = values
        .iter()
        .map(|entry| {
            OciTagSnapshot::new(
                &entry.scope_namespace,
                &entry.repository,
                &entry.tag,
                Some(entry.digest_hex.clone()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let operations = snapshots
        .iter()
        .map(OciTagSnapshot::evidence_operation)
        .collect::<Result<Vec<_>, _>>()?;
    let operation_ids = operations
        .iter()
        .map(|operation| operation.operation_id.clone())
        .collect::<Vec<_>>();
    let rows = query(
        "SELECT DISTINCT ON (current.operation_id)
                current.operation_id, current.sequence, current.event_json,
                current.merkle_commit_json,
                (
                    SELECT previous.merkle_commit_json
                    FROM shardline_reliability_events AS previous
                    WHERE previous.operation_kind = $1
                      AND previous.operation_id = current.operation_id
                      AND previous.sequence < current.sequence
                      AND previous.merkle_commit_json IS NOT NULL
                    ORDER BY previous.sequence DESC
                    LIMIT 1
                ) AS previous_merkle_commit_json
         FROM shardline_reliability_events AS current
         WHERE current.operation_kind = $1
           AND current.operation_id = ANY($2)
         ORDER BY current.operation_id, current.sequence DESC",
    )
    .bind(OperationKind::OciTag.as_str())
    .bind(&operation_ids)
    .fetch_all(&mut *connection)
    .await?;
    let mut latest = HashMap::with_capacity(rows.len());
    for row in rows {
        let operation_id: String = row.try_get("operation_id")?;
        latest.insert(
            operation_id,
            (
                row.try_get::<i64, _>("sequence")?,
                row.try_get::<serde_json::Value, _>("event_json")?,
                row.try_get::<Option<serde_json::Value>, _>("merkle_commit_json")?,
                row.try_get::<Option<serde_json::Value>, _>("previous_merkle_commit_json")?,
            ),
        );
    }
    for (snapshot, operation) in snapshots.into_iter().zip(operations) {
        let operation_id = &operation.operation_id;
        let Some((row_sequence, event_json, merkle_json, previous_merkle_json)) =
            latest.remove(operation_id)
        else {
            return Err(PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ));
        };
        let event_sequence = persisted_event_sequence(OperationKind::OciTag, event_json.clone())?;
        if row_sequence != u64_to_i64(event_sequence)? {
            return Err(PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::Merkle(
                    "OCI tag listing evidence sequence mismatch".into(),
                ),
            ));
        }
        verify_persisted_merkle_commit_with_previous(
            OperationKind::OciTag,
            event_json.clone(),
            merkle_json,
            previous_merkle_json,
        )?;
        let event: OciTagLifecycleEvent = from_value(event_json)?;
        verify_snapshot_event(&event, &snapshot)?;
        if event.operation != operation {
            return Err(PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ));
        }
    }
    Ok(())
}

async fn persist_tag_evidence(
    connection: &mut PgConnection,
    evidence: &OciTagEvidenceLog,
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
    .fetch_one(&mut *connection)
    .await?;
    let persisted_sequence = persisted_sequence.unwrap_or(-1);
    if let Some(event) = evidence.events().last()
        && persisted_sequence >= 0
    {
        let event_sequence = u64_to_i64(event.sequence)?;
        if event_sequence <= persisted_sequence {
            return Ok(());
        }
        if event_sequence == persisted_sequence.saturating_add(1) {
            persist_tag_event(connection, event).await?;
            return Ok(());
        }
    }
    for event in evidence.events() {
        if u64_to_i64(event.sequence)? <= persisted_sequence {
            continue;
        }
        persist_tag_event(connection, event).await?;
    }
    Ok(())
}

async fn persist_tag_event(
    connection: &mut PgConnection,
    event: &OciTagLifecycleEvent,
) -> Result<(), PostgresMetadataStoreError> {
    event.verify_integrity()?;
    let sequence = u64_to_i64(event.sequence)?;
    let previous_json: Option<serde_json::Value> = query_scalar(
        "SELECT merkle_commit_json
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2 AND sequence < $3
           AND merkle_commit_json IS NOT NULL
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(event.operation.kind.as_str())
    .bind(&event.operation.operation_id)
    .bind(sequence)
    .fetch_optional(&mut *connection)
    .await?;
    let previous = previous_json
        .map(serde_json::from_value::<ReliabilityMerkleCommit>)
        .transpose()?;
    let merkle_commit_json =
        reliability_merkle_commit_json_with_previous(event, previous.as_ref())?;
    let result = query(
        "INSERT INTO shardline_reliability_events
            (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds,
             merkle_commit_json)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (operation_kind, operation_id, sequence) DO UPDATE
         SET merkle_commit_json = COALESCE(
             shardline_reliability_events.merkle_commit_json,
             EXCLUDED.merkle_commit_json
         )
         WHERE shardline_reliability_events.event_json = EXCLUDED.event_json",
    )
    .bind(event.operation.kind.as_str())
    .bind(&event.operation.operation_id)
    .bind(sequence)
    .bind(to_value(event)?)
    .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
    .bind(merkle_commit_json)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() == 0 {
        return Err(PostgresMetadataStoreError::ReliabilityEventConflict(
            event.operation.operation_id.clone(),
        ));
    }
    Ok(())
}

pub(super) async fn record_tag_transition(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    before: Option<String>,
    after: Option<String>,
) -> Result<(), PostgresMetadataStoreError> {
    let before_snapshot = OciTagSnapshot::new(scope_namespace, repository, tag, before.clone())?;
    let after_snapshot = OciTagSnapshot::new(scope_namespace, repository, tag, after)?;
    let evidence = verify_and_append_snapshot_transition(
        current_tag_evidence(connection, scope_namespace, repository, tag, before).await?,
        before_snapshot,
        after_snapshot,
    )?
    .0;
    persist_tag_evidence(connection, &evidence).await
}

pub(super) async fn current_tag(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
) -> Result<Option<OciTagEntry>, PostgresMetadataStoreError> {
    query(
        "SELECT scope_namespace, repository, tag, digest_hex
         FROM shardline_oci_tags
         WHERE scope_namespace = $1 AND repository = $2 AND tag = $3",
    )
    .bind(scope_namespace)
    .bind(repository)
    .bind(tag)
    .fetch_optional(&mut *connection)
    .await?
    .as_ref()
    .map(entry_from_row)
    .transpose()
}

async fn lock_oci_tag(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
) -> Result<(), PostgresMetadataStoreError> {
    query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("oci-tag:{scope_namespace}:{repository}:{tag}"))
        .execute(&mut *connection)
        .await?;
    Ok(())
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
        lock_oci_tag(
            connection,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?;
        let before = current_tag(
            connection,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?;
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
        .execute(&mut *connection)
        .await?;
        record_tag_transition(
            connection,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
            before.map(|value| value.digest_hex),
            Some(entry.digest_hex.clone()),
        )
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
        lock_oci_tag(connection, scope_namespace, repository, tag).await?;
        let result = query(
            "DELETE FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3 AND digest_hex = $4",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .bind(digest_hex)
        .execute(&mut *connection)
        .await?;
        if result.rows_affected() == 1 {
            record_tag_transition(
                connection,
                scope_namespace,
                repository,
                tag,
                Some(digest_hex.to_owned()),
                None,
            )
            .await?;
        }
        Ok(result.rows_affected() == 1)
    }
}

#[async_trait::async_trait]
impl OciTagStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn upsert_oci_tag(&self, entry: &OciTagEntry) -> Result<(), Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_oci_tag(
            &mut transaction,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?;
        let before = current_tag(
            &mut transaction,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?;
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
        .execute(&mut *transaction)
        .await?;
        record_tag_transition(
            &mut transaction,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
            before.map(|value| value.digest_hex),
            Some(entry.digest_hex.clone()),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn insert_oci_tag_if_absent(&self, entry: &OciTagEntry) -> Result<bool, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_oci_tag(
            &mut transaction,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?;
        let result = query(
            "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (scope_namespace, repository, tag) DO NOTHING",
        )
        .bind(&entry.scope_namespace)
        .bind(&entry.repository)
        .bind(&entry.tag)
        .bind(&entry.digest_hex)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            record_tag_transition(
                &mut transaction,
                &entry.scope_namespace,
                &entry.repository,
                &entry.tag,
                None,
                Some(entry.digest_hex.clone()),
            )
            .await?;
        } else if let Some(current) = current_tag(
            &mut transaction,
            &entry.scope_namespace,
            &entry.repository,
            &entry.tag,
        )
        .await?
        {
            current_tag_evidence(
                &mut transaction,
                &current.scope_namespace,
                &current.repository,
                &current.tag,
                Some(current.digest_hex),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let value = query(
            "SELECT scope_namespace, repository, tag, digest_hex
             FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .fetch_optional(&mut *transaction)
        .await?
        .as_ref()
        .map(entry_from_row)
        .transpose()?;
        if let Some(value) = value.as_ref() {
            verify_tag_evidence(
                &mut transaction,
                scope_namespace,
                repository,
                tag,
                Some(value.digest_hex.clone()),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(value)
    }

    async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        let limit = u64_to_i64(u64::try_from(limit).unwrap_or(u64::MAX))?;
        let mut transaction = self.pool.begin().await?;
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
            .fetch_all(&mut *transaction)
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
            .fetch_all(&mut *transaction)
            .await?
        };
        let values: Vec<_> = rows.iter().map(entry_from_row).collect::<Result<_, _>>()?;
        verify_tag_listing_evidence(&mut transaction, &values).await?;
        transaction.commit().await?;
        Ok(values)
    }

    async fn list_oci_tags_by_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        digest_hex: &str,
    ) -> Result<Vec<OciTagEntry>, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        let rows = query(
            "SELECT scope_namespace, repository, tag, digest_hex
             FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3
             ORDER BY tag",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(digest_hex)
        .fetch_all(&mut *transaction)
        .await?;
        let values: Vec<_> = rows.iter().map(entry_from_row).collect::<Result<_, _>>()?;
        verify_tag_listing_evidence(&mut transaction, &values).await?;
        transaction.commit().await?;
        Ok(values)
    }

    async fn delete_oci_tag_if_digest(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
        digest_hex: &str,
    ) -> Result<bool, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_oci_tag(&mut transaction, scope_namespace, repository, tag).await?;
        let result = query(
            "DELETE FROM shardline_oci_tags
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3 AND digest_hex = $4",
        )
        .bind(scope_namespace)
        .bind(repository)
        .bind(tag)
        .bind(digest_hex)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            record_tag_transition(
                &mut transaction,
                scope_namespace,
                repository,
                tag,
                Some(digest_hex.to_owned()),
                None,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        super::super::connect_isolated_postgres().await
    }

    fn entry(digest: &str) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-pg-cas".to_owned(),
            repository: "team/assets".to_owned(),
            tag: "latest".to_owned(),
            digest_hex: digest.to_owned(),
        }
    }

    async fn cleanup_tags(pool: &sqlx::PgPool, scope: &str) {
        sqlx::query(
            "WITH fixture_rows AS (
                 SELECT t.scope_namespace, t.repository, t.tag,
                        COALESCE(MAX(e.sequence), -1) + 1 AS next_sequence
                 FROM shardline_oci_tags AS t
                 LEFT JOIN shardline_reliability_events AS e
                   ON e.operation_kind = 'OciTag'
                  AND e.operation_id = octet_length(t.scope_namespace)::text || ':' || t.scope_namespace
                      || octet_length(t.repository)::text || ':' || t.repository
                      || octet_length(t.tag)::text || ':' || t.tag
                 WHERE t.scope_namespace = $1
                 GROUP BY t.scope_namespace, t.repository, t.tag
             )
             INSERT INTO shardline_reliability_events
                 (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
             SELECT 'OciTag',
                    octet_length(scope_namespace)::text || ':' || scope_namespace
                        || octet_length(repository)::text || ':' || repository
                        || octet_length(tag)::text || ':' || tag,
                    next_sequence,
                    jsonb_build_object(
                        'after', jsonb_build_object(
                            'scope_namespace', scope_namespace,
                            'repository', repository,
                            'tag', tag,
                            'digest_hex', NULL
                        )
                    ),
                    EXTRACT(EPOCH FROM now())::bigint
             FROM fixture_rows
             ON CONFLICT (operation_kind, operation_id, sequence) DO NOTHING",
        )
        .bind(scope)
        .execute(pool)
        .await
        .expect("seed OCI tag deletion evidence");
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind(scope)
            .execute(pool)
            .await
            .expect("clean OCI tag fixture");
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id LIKE $2 || '%'",
        )
        .bind(shardline_reliability::OperationKind::OciTag.as_str())
        .bind(format!("{}:{scope}", scope.len()))
        .execute(pool)
        .await
        .expect("clean OCI tag evidence fixture");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_tag_delete_cannot_remove_concurrent_retarget() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        cleanup_tags(&pool, "oci-pg-cas").await;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_tag_read_rejects_tampered_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        cleanup_tags(&pool, "oci-pg-tamper").await;
        let store = PostgresIndexStore::new(pool.clone());
        let value = OciTagEntry {
            scope_namespace: "oci-pg-tamper".to_owned(),
            repository: "team/assets".to_owned(),
            tag: "latest".to_owned(),
            digest_hex: "a".repeat(64),
        };
        store.upsert_oci_tag(&value).await.unwrap();
        query(
            "UPDATE shardline_reliability_events
             SET event_json = '{\"tampered\":true}'
             WHERE operation_kind = 'OciTag'
               AND event_json->'operation'->>'tenant' = $1",
        )
        .bind(&value.scope_namespace)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            store
                .oci_tag(&value.scope_namespace, &value.repository, &value.tag)
                .await
                .is_err()
        );
        cleanup_tags(&pool, &value.scope_namespace).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_tag_read_rejects_missing_baseline_evidence_without_writing() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = "oci-pg-repair";
        cleanup_tags(&pool, scope).await;
        let store = PostgresIndexStore::new(pool.clone());
        let value = OciTagEntry {
            scope_namespace: scope.to_owned(),
            repository: "team/assets".to_owned(),
            tag: "latest".to_owned(),
            digest_hex: "a".repeat(64),
        };
        store.upsert_oci_tag(&value).await.unwrap();
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2",
        )
        .bind(shardline_reliability::OperationKind::OciTag.as_str())
        .bind(scope)
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            store
                .oci_tag(&value.scope_namespace, &value.repository, &value.tag)
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM shardline_reliability_events
             WHERE operation_kind = $1
               AND event_json->'operation'->>'tenant' = $2",
        )
        .bind(shardline_reliability::OperationKind::OciTag.as_str())
        .bind(scope)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
        cleanup_tags(&pool, scope).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_same_tag_legacy_writer_is_rejected_by_reliability_gate() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope = format!("oci-pg-mixed-version-{}", std::process::id());
        cleanup_tags(&pool, &scope).await;
        let store = PostgresIndexStore::new(pool.clone());
        let value = OciTagEntry {
            scope_namespace: scope.clone(),
            repository: "team/assets".to_owned(),
            tag: "latest".to_owned(),
            digest_hex: "a".repeat(64),
        };
        store.upsert_oci_tag(&value).await.expect("seed tag");

        let mut transaction = pool.begin().await.expect("begin legacy transaction");
        query(
            "UPDATE shardline_oci_tags
             SET digest_hex = $4
             WHERE scope_namespace = $1 AND repository = $2 AND tag = $3",
        )
        .bind(&scope)
        .bind(&value.repository)
        .bind(&value.tag)
        .bind("b".repeat(64))
        .execute(&mut *transaction)
        .await
        .expect("legacy write reaches deferred gate");
        assert!(transaction.commit().await.is_err());
        assert_eq!(
            store
                .oci_tag(&scope, &value.repository, &value.tag)
                .await
                .expect("read after rejected write"),
            Some(value)
        );
        cleanup_tags(&pool, &scope).await;
    }
}
