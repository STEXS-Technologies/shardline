use serde_json::to_value;
use shardline_reliability::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleEvent, OciObjectLifecycleState,
    OciObjectSnapshot, resumable_session_event,
};
use sqlx::{Connection as _, PgConnection, Row as _, query, query_scalar};

use super::{
    PostgresIndexStore, PostgresMetadataStoreError, insert_reliability_event_json,
    next_reliability_sequence,
};

fn oci_snapshot(
    key: &OciObjectKey,
    state: OciObjectLifecycleState,
    deleted_at: Option<u64>,
) -> Result<OciObjectSnapshot, PostgresMetadataStoreError> {
    Ok(OciObjectSnapshot::new(
        OciObjectIdentity::new(
            key.scope_namespace.clone(),
            key.repository.clone(),
            key.kind.as_str(),
            key.digest_hex.clone(),
        )?,
        state,
        deleted_at,
    )?)
}

async fn load_oci_evidence(
    executor: &mut sqlx::PgConnection,
    key: &OciObjectKey,
) -> Result<OciObjectEvidenceLog, PostgresMetadataStoreError> {
    let operation_id = format!(
        "{}:{}:{}:{}",
        key.scope_namespace,
        key.repository,
        key.kind.as_str(),
        key.digest_hex
    );
    let rows = query(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'Visibility' AND operation_id = $1 ORDER BY sequence",
    )
    .bind(operation_id)
    .fetch_all(executor)
    .await?;
    let events = rows
        .into_iter()
        .map(|row| {
            Ok(serde_json::from_value::<OciObjectLifecycleEvent>(
                row.try_get("event_json")?,
            )?)
        })
        .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
    Ok(OciObjectEvidenceLog::from_events(events)?)
}

async fn record_oci_evidence(
    executor: &mut sqlx::PgConnection,
    key: &OciObjectKey,
    state: OciObjectLifecycleState,
    deleted_at: Option<u64>,
    fallback_state: OciObjectLifecycleState,
    fallback_deleted_at: Option<u64>,
) -> Result<(), PostgresMetadataStoreError> {
    let after = oci_snapshot(key, state, deleted_at)?;
    let mut evidence = load_oci_evidence(executor, key).await?;
    let evidence_was_empty = evidence.events().is_empty();
    if evidence.events().is_empty() {
        evidence = OciObjectEvidenceLog::baseline(oci_snapshot(
            key,
            fallback_state,
            fallback_deleted_at,
        )?)?;
    }
    evidence.record(after)?;
    let event = evidence.events().last().ok_or_else(|| {
        PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::EmptyField("OCI object evidence event"),
        )
    })?;
    if evidence_was_empty {
        for stored_event in evidence.events() {
            insert_reliability_event_json(
                &mut *executor,
                &stored_event.operation,
                stored_event.sequence,
                to_value(stored_event)?,
            )
            .await?;
        }
        Ok(())
    } else {
        insert_reliability_event_json(executor, &event.operation, event.sequence, to_value(event)?)
            .await
    }
}
use crate::{
    OciObjectKey, OciObjectKind, OciObjectStore, OciObjectTombstone, OciTagEntry,
    ResumableCompletionFence,
};

impl PostgresIndexStore {
    /// Publishes an OCI object through the session that owns its repository fence.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be committed.
    pub async fn publish_oci_object_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let previous_deleted_at = query(
            "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2 AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.try_get::<i64, _>("deleted_at_unix_seconds"))
        .transpose()?
        .map(super::i64_to_u64)
        .transpose()?;
        query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        for tag in tags {
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        record_oci_evidence(
            transaction.as_mut(),
            key,
            OciObjectLifecycleState::Published,
            None,
            if previous_deleted_at.is_some() {
                OciObjectLifecycleState::Deleted
            } else {
                OciObjectLifecycleState::Published
            },
            previous_deleted_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Publishes an OCI object and completes its resumable session in the same
    /// transaction, rejecting a superseded completion fence.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres cannot validate or commit the transaction.
    pub async fn publish_oci_object_completion_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
        fence: &ResumableCompletionFence,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let owns_completion = query(
            "SELECT scope_namespace, target_key FROM shardline_resumable_sessions
             WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2
             AND expires_at > clock_timestamp()
             FOR UPDATE",
        )
        .bind(fence.session_id())
        .bind(super::u64_to_i64(fence.epoch().get())?)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(owns_completion) = owns_completion else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let scope_namespace: String = owns_completion.try_get("scope_namespace")?;
        let target_key: String = owns_completion.try_get("target_key")?;
        query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        for tag in tags {
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        record_oci_evidence(
            transaction.as_mut(),
            key,
            OciObjectLifecycleState::Published,
            None,
            OciObjectLifecycleState::Published,
            None,
        )
        .await?;
        let completed = query(
            "UPDATE shardline_resumable_sessions SET state = 'completed', updated_at = now()
             WHERE session_id = $1 AND state = 'completing' AND fence_epoch = $2",
        )
        .bind(fence.session_id())
        .bind(super::u64_to_i64(fence.epoch().get())?)
        .execute(&mut *transaction)
        .await?;
        if completed.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        let sequence = next_reliability_sequence(
            transaction.as_mut(),
            shardline_reliability::OperationKind::ResumableSession,
            fence.session_id(),
        )
        .await?;
        let event = resumable_session_event(
            scope_namespace,
            fence.session_id(),
            target_key,
            sequence,
            crate::ResumableSessionState::Completing,
            crate::ResumableSessionState::Completed,
        )?;
        insert_reliability_event_json(
            transaction.as_mut(),
            &event.operation,
            sequence,
            to_value(&event)?,
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Logically deletes an OCI object through the lock-owning session.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be committed.
    pub async fn delete_oci_object_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let previous_deleted_at = query(
            "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2 AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.try_get::<i64, _>("deleted_at_unix_seconds"))
        .transpose()?
        .map(super::i64_to_u64)
        .transpose()?;
        query(
            "INSERT INTO shardline_oci_object_tombstones
                (scope_namespace, repository, object_kind, digest_hex, deleted_at_unix_seconds)
             VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
             ON CONFLICT (scope_namespace, repository, object_kind, digest_hex)
             DO UPDATE SET deleted_at_unix_seconds = EXCLUDED.deleted_at_unix_seconds",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&mut *transaction)
        .await?;
        if key.kind == OciObjectKind::Manifest {
            query(
                "DELETE FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3",
            )
            .bind(&key.scope_namespace)
            .bind(&key.repository)
            .bind(&key.digest_hex)
            .execute(&mut *transaction)
            .await?;
        }
        let deleted_at: i64 = query_scalar(
            "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2 AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_one(&mut *transaction)
        .await?;
        let deleted_at = super::i64_to_u64(deleted_at)?;
        record_oci_evidence(
            transaction.as_mut(),
            key,
            OciObjectLifecycleState::Deleted,
            Some(deleted_at),
            if previous_deleted_at.is_some() {
                OciObjectLifecycleState::Deleted
            } else {
                OciObjectLifecycleState::Published
            },
            previous_deleted_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl OciObjectStore for PostgresIndexStore {
    type Error = PostgresMetadataStoreError;

    async fn oci_object_is_deleted(&self, key: &OciObjectKey) -> Result<bool, Self::Error> {
        let found = query_scalar::<_, i32>(
            "SELECT 1 FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_optional(&self.pool)
        .await?;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        let evidence = load_oci_evidence(transaction.as_mut(), key).await?;
        if found.is_some() {
            let deleted_at: i64 = query_scalar(
                "SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
                 WHERE scope_namespace = $1 AND repository = $2 AND object_kind = $3 AND digest_hex = $4",
            )
            .bind(&key.scope_namespace)
            .bind(&key.repository)
            .bind(key.kind.as_str())
            .bind(&key.digest_hex)
            .fetch_one(transaction.as_mut())
            .await?;
            let expected = oci_snapshot(
                key,
                OciObjectLifecycleState::Deleted,
                Some(super::i64_to_u64(deleted_at)?),
            )?;
            if evidence.events().is_empty() {
                let baseline = OciObjectEvidenceLog::baseline(expected.clone())?;
                baseline.verify_for(&expected)?;
                for event in baseline.events() {
                    insert_reliability_event_json(
                        transaction.as_mut(),
                        &event.operation,
                        event.sequence,
                        to_value(event)?,
                    )
                    .await?;
                }
            } else {
                evidence.verify_for(&expected)?;
            }
        } else if !evidence.events().is_empty() {
            shardline_reliability::verify_oci_object_lifecycle_chain(evidence.events())?;
        }
        transaction.commit().await?;
        Ok(found.is_some())
    }

    async fn publish_oci_object(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), Self::Error> {
        let mut connection = self.pool.acquire().await?;
        self.publish_oci_object_on_connection(&mut connection, key, tags)
            .await
    }

    async fn delete_oci_object(&self, key: &OciObjectKey) -> Result<(), Self::Error> {
        let mut connection = self.pool.acquire().await?;
        self.delete_oci_object_on_connection(&mut connection, key)
            .await
    }

    async fn list_oci_object_tombstones(&self) -> Result<Vec<OciObjectTombstone>, Self::Error> {
        let rows = query(
            "SELECT scope_namespace, repository, object_kind, digest_hex,
                    deleted_at_unix_seconds
             FROM shardline_oci_object_tombstones
             ORDER BY scope_namespace, repository, object_kind, digest_hex",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        let tombstones = rows
            .into_iter()
            .map(|row| {
                let object_kind: String = row.try_get("object_kind")?;
                let kind = object_kind.parse()?;
                Ok(OciObjectTombstone {
                    key: OciObjectKey {
                        scope_namespace: row.try_get("scope_namespace")?,
                        repository: row.try_get("repository")?,
                        kind,
                        digest_hex: row.try_get("digest_hex")?,
                    },
                    deleted_at_unix_seconds: super::i64_to_u64(
                        row.try_get("deleted_at_unix_seconds")?,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, Self::Error>>()?;
        for tombstone in &tombstones {
            let evidence = load_oci_evidence(transaction.as_mut(), &tombstone.key).await?;
            let expected = oci_snapshot(
                &tombstone.key,
                OciObjectLifecycleState::Deleted,
                Some(tombstone.deleted_at_unix_seconds),
            )?;
            if evidence.events().is_empty() {
                let baseline = OciObjectEvidenceLog::baseline(expected.clone())?;
                baseline.verify_for(&expected)?;
                for event in baseline.events() {
                    insert_reliability_event_json(
                        transaction.as_mut(),
                        &event.operation,
                        event.sequence,
                        to_value(event)?,
                    )
                    .await?;
                }
            } else {
                evidence.verify_for(&expected)?;
            }
        }
        transaction.commit().await?;
        Ok(tombstones)
    }

    async fn delete_oci_object_tombstone_if_unchanged(
        &self,
        tombstone: &OciObjectTombstone,
    ) -> Result<bool, Self::Error> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        let result = query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4
               AND deleted_at_unix_seconds = $5",
        )
        .bind(&tombstone.key.scope_namespace)
        .bind(&tombstone.key.repository)
        .bind(tombstone.key.kind.as_str())
        .bind(&tombstone.key.digest_hex)
        .bind(super::u64_to_i64(tombstone.deleted_at_unix_seconds)?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 0 {
            record_oci_evidence(
                transaction.as_mut(),
                &tombstone.key,
                OciObjectLifecycleState::Reclaimed,
                Some(tombstone.deleted_at_unix_seconds),
                OciObjectLifecycleState::Deleted,
                Some(tombstone.deleted_at_unix_seconds),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(result.rows_affected() != 0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{OciTagStore as _, ResumableSession, ResumableSessionProtocol};
    use sqlx::query_as;
    use std::time::Duration;

    async fn connect_postgres() -> Option<sqlx::PgPool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        sqlx::PgPool::connect(&url).await.ok()
    }

    fn object() -> OciObjectKey {
        OciObjectKey {
            scope_namespace: "oci-tombstone-pg".to_owned(),
            repository: "team/assets".to_owned(),
            kind: OciObjectKind::Manifest,
            digest_hex: "a".repeat(64),
        }
    }

    fn tag(name: &str, digest: char) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: "oci-tombstone-pg".to_owned(),
            repository: "team/assets".to_owned(),
            tag: name.to_owned(),
            digest_hex: digest.to_string().repeat(64),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_manifest_tombstone_and_tags_share_one_commit() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        query("DELETE FROM shardline_oci_object_tombstones WHERE scope_namespace = $1")
            .bind("oci-tombstone-pg")
            .execute(&pool)
            .await
            .unwrap();
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind("oci-tombstone-pg")
            .execute(&pool)
            .await
            .unwrap();
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility'
               AND operation_id = $1",
        )
        .bind(format!(
            "{}:{}:{}:{}",
            "oci-tombstone-pg",
            "team/assets",
            OciObjectKind::Manifest.as_str(),
            "a".repeat(64)
        ))
        .execute(&pool)
        .await
        .unwrap();
        let store = PostgresIndexStore::new(pool);
        let manifest = object();
        let current = tag("latest", 'a');
        let unrelated = tag("stable", 'b');

        store
            .publish_oci_object(&manifest, &[current.clone(), unrelated.clone()])
            .await
            .unwrap();
        store.delete_oci_object(&manifest).await.unwrap();

        assert!(store.oci_object_is_deleted(&manifest).await.unwrap());
        assert!(
            store
                .oci_tag(&current.scope_namespace, &current.repository, &current.tag)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .oci_tag(
                    &unrelated.scope_namespace,
                    &unrelated.repository,
                    &unrelated.tag,
                )
                .await
                .unwrap(),
            Some(unrelated)
        );

        let tombstone = store
            .list_oci_object_tombstones()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.key == manifest)
            .unwrap();
        let mut stale = tombstone.clone();
        stale.deleted_at_unix_seconds = stale.deleted_at_unix_seconds.saturating_add(1);
        assert!(
            !store
                .delete_oci_object_tombstone_if_unchanged(&stale)
                .await
                .unwrap()
        );
        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_reclaim_repairs_missing_visibility_baseline_chain() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = OciObjectKey {
            scope_namespace: "oci-reclaim-repair".to_owned(),
            repository: "team/assets".to_owned(),
            kind: OciObjectKind::Blob,
            digest_hex: "f".repeat(64),
        };
        query(
            "DELETE FROM shardline_oci_object_tombstones
             WHERE scope_namespace = $1 AND repository = $2
               AND object_kind = $3 AND digest_hex = $4",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .execute(&pool)
        .await
        .unwrap();
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility' AND operation_id = $1",
        )
        .bind(format!(
            "{}:{}:{}:{}",
            key.scope_namespace,
            key.repository,
            key.kind.as_str(),
            key.digest_hex
        ))
        .execute(&pool)
        .await
        .unwrap();
        store.delete_oci_object(&key).await.unwrap();
        let tombstone = store
            .list_oci_object_tombstones()
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.key == key)
            .unwrap();
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility' AND operation_id = $1",
        )
        .bind(format!(
            "{}:{}:{}:{}",
            key.scope_namespace,
            key.repository,
            key.kind.as_str(),
            key.digest_hex
        ))
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            store
                .delete_oci_object_tombstone_if_unchanged(&tombstone)
                .await
                .unwrap()
        );
        let (count, minimum, maximum): (i64, i64, i64) = query_as(
            "SELECT COUNT(*), MIN(sequence), MAX(sequence)
             FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility' AND operation_id = $1",
        )
        .bind(format!(
            "{}:{}:{}:{}",
            key.scope_namespace,
            key.repository,
            key.kind.as_str(),
            key.digest_hex
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((count, minimum, maximum), (2, 0, 1));
        query(
            "DELETE FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility' AND operation_id = $1",
        )
        .bind(format!(
            "{}:{}:{}:{}",
            key.scope_namespace,
            key.repository,
            key.kind.as_str(),
            key.digest_hex
        ))
        .execute(&pool)
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_oci_completion_records_the_resumable_transition_atomically() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let session = ResumableSession::new(
            format!("oci-completion-{suffix}"),
            ResumableSessionProtocol::OciBlob,
            format!("oci-completion-scope-{suffix}"),
            "team/assets".to_owned(),
            Duration::from_secs(u64::try_from(chrono::Utc::now().timestamp()).unwrap() + 3_600),
        );
        let store = PostgresIndexStore::new(pool);
        assert!(store.create_resumable_session(&session).await.unwrap());
        let claimed = store
            .begin_resumable_completion(session.session_id())
            .await
            .unwrap()
            .unwrap()
            .0;
        let key = OciObjectKey {
            scope_namespace: session.scope_namespace().to_owned(),
            repository: session.target_key().to_owned(),
            kind: OciObjectKind::Manifest,
            digest_hex: "b".repeat(64),
        };
        let mut connection = store.pool().acquire().await.unwrap();
        assert!(
            store
                .publish_oci_object_completion_on_connection(
                    &mut connection,
                    &key,
                    &[],
                    &claimed.completion_fence(),
                )
                .await
                .unwrap()
        );
        let events = store
            .resumable_reliability_events(session.session_id())
            .await
            .unwrap();
        shardline_reliability::verify_state_transition_chain(&events).unwrap();
        assert_eq!(
            events.last().unwrap().after,
            crate::ResumableSessionState::Completed
        );
    }
}
