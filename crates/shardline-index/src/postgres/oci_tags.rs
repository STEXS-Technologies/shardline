use serde_json::{from_value, to_value};
use sqlx::{PgConnection, Row, postgres::PgRow, query};

use super::{PostgresIndexStore, PostgresMetadataStoreError, u64_to_i64};
use crate::{OciTagEntry, OciTagStore};
use shardline_reliability::{
    OciTagEvidenceLog, OciTagLifecycleEvent, OciTagSnapshot, SnapshotEvidence,
    verify_oci_tag_events,
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
    let rows = query(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'OciTag' AND operation_id = $1 ORDER BY sequence",
    )
    .bind(&operation.operation_id)
    .fetch_all(&mut *connection)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let value: serde_json::Value = row.try_get("event_json")?;
        events.push(from_value::<OciTagLifecycleEvent>(value)?);
    }
    Ok(OciTagEvidenceLog::from_events(events)?)
}

async fn current_tag_evidence(
    connection: &mut PgConnection,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    digest_hex: Option<String>,
) -> Result<OciTagEvidenceLog, PostgresMetadataStoreError> {
    let snapshot = OciTagSnapshot::new(scope_namespace, repository, tag, digest_hex)?;
    let evidence = load_tag_evidence(connection, scope_namespace, repository, tag).await?;
    if evidence.events().is_empty() {
        return Ok(OciTagEvidenceLog::baseline(snapshot)?);
    }
    verify_oci_tag_events(evidence.events(), &snapshot)?;
    Ok(evidence)
}

async fn persist_tag_evidence(
    connection: &mut PgConnection,
    evidence: &OciTagEvidenceLog,
) -> Result<(), PostgresMetadataStoreError> {
    for event in evidence.events() {
        query(
            "INSERT INTO shardline_reliability_events
                (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (operation_kind, operation_id, sequence) DO NOTHING",
        )
        .bind(event.operation.kind.as_str())
        .bind(&event.operation.operation_id)
        .bind(u64_to_i64(event.sequence)?)
        .bind(to_value(event)?)
        .bind(shardline_protocol::unix_now_seconds_lossy() as i64)
        .execute(&mut *connection)
        .await?;
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
    let mut evidence =
        current_tag_evidence(connection, scope_namespace, repository, tag, before).await?;
    evidence.record(OciTagSnapshot::new(
        scope_namespace,
        repository,
        tag,
        after,
    )?)?;
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
        current_tag_evidence(
            &mut transaction,
            scope_namespace,
            repository,
            tag,
            value.as_ref().map(|entry| entry.digest_hex.clone()),
        )
        .await?;
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
        for entry in &values {
            current_tag_evidence(
                &mut transaction,
                &entry.scope_namespace,
                &entry.repository,
                &entry.tag,
                Some(entry.digest_hex.clone()),
            )
            .await?;
        }
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
        for entry in &values {
            current_tag_evidence(
                &mut transaction,
                &entry.scope_namespace,
                &entry.repository,
                &entry.tag,
                Some(entry.digest_hex.clone()),
            )
            .await?;
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_tag_read_rejects_tampered_evidence() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind("oci-pg-tamper")
            .execute(&pool)
            .await
            .unwrap();
        query("DELETE FROM shardline_reliability_events WHERE operation_kind = 'OciTag'")
            .execute(&pool)
            .await
            .unwrap();
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
             WHERE operation_kind = 'OciTag'",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            store
                .oci_tag(&value.scope_namespace, &value.repository, &value.tag)
                .await
                .is_err()
        );
    }
}
