use shardline_reliability::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleState, OciObjectOperationId,
    OciObjectSnapshot, OperationKind, resumable_session_event,
    verify_and_append_snapshot_transition, verify_snapshot_evidence,
};
use sqlx::{Connection as _, PgConnection, Row as _, query, query_scalar};

use super::{
    PostgresIndexStore, PostgresMetadataStoreError, insert_reliability_event,
    load_postgres_latest_evidence_heads, next_reliability_sequence,
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
    let operation_id = OciObjectOperationId::new(&OciObjectIdentity::new(
        key.scope_namespace.clone(),
        key.repository.clone(),
        key.kind.as_str(),
        key.digest_hex.clone(),
    )?);
    let event = super::index_store::load_postgres_latest_evidence_event(
        &mut *executor,
        OperationKind::Visibility,
        operation_id.as_str(),
    )
    .await?;
    let Some(event) = event else {
        return Ok(OciObjectEvidenceLog::default());
    };
    Ok(OciObjectEvidenceLog::from_head(serde_json::from_value(
        event,
    )?)?)
}

async fn record_oci_evidence(
    executor: &mut sqlx::PgConnection,
    key: &OciObjectKey,
    state: OciObjectLifecycleState,
    deleted_at: Option<u64>,
    fallback_state: OciObjectLifecycleState,
    fallback_deleted_at: Option<u64>,
) -> Result<(), PostgresMetadataStoreError> {
    if state == OciObjectLifecycleState::Published
        && query_scalar::<_, bool>(
            "SELECT to_regprocedure(format('%I.shardline_oci_publication_remote_delete_v1(text,text,text,text,text)', current_schema())) IS NOT NULL",
        ).fetch_one(&mut *executor).await?
    {
        query("SELECT shardline_oci_publication_remote_delete_v1(current_schema(), $1, $2, $3, $4)")
            .bind(&key.scope_namespace).bind(&key.repository).bind(key.kind.as_str()).bind(&key.digest_hex)
            .execute(&mut *executor).await?;
    }
    let after = oci_snapshot(key, state, deleted_at)?;
    let evidence = load_oci_evidence(executor, key).await?;
    let fallback = if state == OciObjectLifecycleState::Published
        && fallback_state == OciObjectLifecycleState::Published
        && fallback_deleted_at.is_none()
        && let Some(head) = evidence.events().last()
        && head.after.state != OciObjectLifecycleState::Published
    {
        // Staged and reclaimed bytes have verified hidden state without a
        // tombstone row. Publication must transition from that actual state.
        oci_snapshot(key, head.after.state, head.after.deleted_at_unix_seconds)?
    } else {
        oci_snapshot(key, fallback_state, fallback_deleted_at)?
    };
    let (evidence, evidence_was_empty) =
        verify_and_append_snapshot_transition(evidence, fallback, after)?;
    let event = evidence.events().last().ok_or_else(|| {
        PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::EmptyField("OCI object evidence event"),
        )
    })?;
    if evidence_was_empty {
        for stored_event in evidence.events() {
            insert_reliability_event(&mut *executor, stored_event).await?;
        }
        Ok(())
    } else {
        insert_reliability_event(executor, event).await
    }
}
use crate::{
    OciObjectKey, OciObjectKind, OciObjectStore, OciObjectTombstone, OciTagEntry,
    ResumableCompletionFence,
};

async fn check_oci_publication_references(
    connection: &mut PgConnection,
    key: &OciObjectKey,
    references: &[OciObjectKey],
) -> Result<(), PostgresMetadataStoreError> {
    if query_scalar::<_, bool>(
        "SELECT to_regprocedure(format('%I.shardline_oci_publication_references_remote_delete_v1(text,jsonb)', current_schema())) IS NOT NULL",
    ).fetch_one(&mut *connection).await? {
        let identities: Vec<_> = std::iter::once(key).chain(references).map(|identity| {
            serde_json::json!({
                "scope_namespace": identity.scope_namespace,
                "repository": identity.repository,
                "object_kind": identity.kind.as_str(),
                "digest_hex": identity.digest_hex,
            })
        }).collect();
        query("SELECT shardline_oci_publication_references_remote_delete_v1(current_schema(), $1)")
            .bind(serde_json::Value::Array(identities)).execute(connection).await?;
    }
    Ok(())
}

impl PostgresIndexStore {
    /// Restores at most 64 hidden manifests to the ordinary GC inventory.
    ///
    /// The pool must retain the session owning the exclusive GC barrier. The
    /// installed function checks that session and existing read/delete grants;
    /// it never bypasses active row policies for the GC account. Candidates
    /// still require normal native evidence verification before reclamation.
    /// `None` means the owner upgrade is absent, existing OCI mutation grants
    /// are insufficient, or row-scoped GC is unsupported.
    ///
    /// # Errors
    /// Returns missing ownership, a schema mismatch or database failure.
    pub async fn restore_staged_oci_tombstones_for_gc(
        &self,
    ) -> Result<Option<u64>, PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let (schema, installed): (String, bool) = sqlx::query_as(
            "SELECT current_schema(), to_regprocedure(format('%I.shardline_oci_staged_gc_inventory_v1(text,integer)',current_schema())) IS NOT NULL",
        ).fetch_one(&mut *transaction).await?;
        if !installed {
            return Ok(None);
        }
        let schema = schema.replace('"', "\"\"");
        let restored: i64 = query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT \"{schema}\".shardline_oci_staged_gc_inventory_v1(current_schema(),64)"
        )))
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if restored < 0 {
            return Ok(None);
        }
        Ok(Some(super::i64_to_u64(restored)?))
    }

    /// Keeps new immutable OCI bytes hidden until their publication commits.
    /// Records the hidden state in the existing evidence log without requiring
    /// extra tombstone-table privileges. Existing published identities and
    /// deletion generations are preserved.
    /// Call only when the physical manifest did not exist under its repository
    /// write fence; legacy objects with existing bytes need no staging.
    ///
    /// # Errors
    ///
    /// Returns an error for pending references, invalid evidence or database failure.
    pub async fn stage_oci_object_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        references: &[OciObjectKey],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        check_oci_publication_references(transaction.as_mut(), key, references).await?;
        let evidence = load_oci_evidence(transaction.as_mut(), key).await?;
        if let Some(head) = evidence.events().last() {
            let expected = oci_snapshot(key, head.after.state, head.after.deleted_at_unix_seconds)?;
            verify_snapshot_evidence(&evidence, &expected)?;
            if matches!(
                head.after.state,
                OciObjectLifecycleState::Published | OciObjectLifecycleState::Deleted
            ) {
                transaction.commit().await?;
                return Ok(());
            }
        }
        let deleted_at: i64 = query_scalar(
            "SELECT COALESCE((SELECT deleted_at_unix_seconds FROM shardline_oci_object_tombstones
             WHERE scope_namespace=$1 AND repository=$2 AND object_kind=$3 AND digest_hex=$4),
             EXTRACT(EPOCH FROM NOW())::BIGINT)",
        )
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(key.kind.as_str())
        .bind(&key.digest_hex)
        .fetch_one(transaction.as_mut())
        .await?;
        let deleted_at = super::i64_to_u64(deleted_at)?;
        let fallback = evidence.events().last().map_or(
            (OciObjectLifecycleState::Deleted, Some(deleted_at)),
            |head| (head.after.state, head.after.deleted_at_unix_seconds),
        );
        record_oci_evidence(
            transaction.as_mut(),
            key,
            OciObjectLifecycleState::Deleted,
            Some(deleted_at),
            fallback.0,
            fallback.1,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
    /// Publishes an OCI object through the session that owns its repository fence.
    ///
    /// The caller must acquire and retain that fence; supplying a connection
    /// does not acquire ownership. This compatibility method supplies no payload
    /// references and does not inspect backing object bytes. For manifests,
    /// validate the payload and use
    /// [`Self::publish_oci_object_with_references_on_connection`] with its full
    /// reference set while retaining the original fence and storage protection.
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
        self.publish_oci_object_with_references_on_connection(connection, key, tags, &[])
            .await
    }

    /// Publishes an OCI object and checks validated payload references in the
    /// same transaction. The original publication API remains available.
    ///
    /// The caller must retain the repository fence on this connection and
    /// provide the complete reference set from a validated payload. These checks
    /// coordinate metadata with pending deletes; they do not read object storage
    /// or establish backing-byte presence. Keep storage validation and reference
    /// protection in the same owned operation. This method never acquires a
    /// replacement connection when the supplied connection fails.
    ///
    /// # Errors
    ///
    /// Returns an error if a referenced object has an unresolved delete or the
    /// publication transaction cannot commit.
    pub async fn publish_oci_object_with_references_on_connection(
        &self,
        connection: &mut PgConnection,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
        references: &[OciObjectKey],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        if !references.is_empty() {
            check_oci_publication_references(transaction.as_mut(), key, references).await?;
        }
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
            let before = super::oci_tags::current_tag(
                transaction.as_mut(),
                &tag.scope_namespace,
                &tag.repository,
                &tag.tag,
            )
            .await?;
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex
                 WHERE shardline_oci_tags.digest_hex IS DISTINCT FROM EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
            super::oci_tags::record_tag_transition(
                transaction.as_mut(),
                &tag.scope_namespace,
                &tag.repository,
                &tag.tag,
                before.map(|value| value.digest_hex),
                Some(tag.digest_hex.clone()),
            )
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
        // Tag rows and their integrity-checked pointer histories share this
        // completion transaction, so a published manifest cannot expose an
        // unjournaled retarget.
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
            let before = super::oci_tags::current_tag(
                transaction.as_mut(),
                &tag.scope_namespace,
                &tag.repository,
                &tag.tag,
            )
            .await?;
            query(
                "INSERT INTO shardline_oci_tags (scope_namespace, repository, tag, digest_hex)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (scope_namespace, repository, tag)
                 DO UPDATE SET digest_hex = EXCLUDED.digest_hex
                 WHERE shardline_oci_tags.digest_hex IS DISTINCT FROM EXCLUDED.digest_hex",
            )
            .bind(&tag.scope_namespace)
            .bind(&tag.repository)
            .bind(&tag.tag)
            .bind(&tag.digest_hex)
            .execute(&mut *transaction)
            .await?;
            super::oci_tags::record_tag_transition(
                transaction.as_mut(),
                &tag.scope_namespace,
                &tag.repository,
                &tag.tag,
                before.map(|value| value.digest_hex),
                Some(tag.digest_hex.clone()),
            )
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
        super::refresh_resumable_state_digest(&mut transaction, fence.session_id()).await?;
        let sequence = next_reliability_sequence(
            transaction.as_mut(),
            shardline_reliability::OperationKind::ResumableSession,
            fence.session_id(),
        )
        .await?;
        let event = resumable_session_event(
            &scope_namespace,
            fence.session_id(),
            target_key,
            sequence,
            crate::ResumableSessionState::Completing,
            crate::ResumableSessionState::Completed,
        )?;
        insert_reliability_event(transaction.as_mut(), &event).await?;
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
            let tags = query(
                "SELECT scope_namespace, repository, tag, digest_hex
                 FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3",
            )
            .bind(&key.scope_namespace)
            .bind(&key.repository)
            .bind(&key.digest_hex)
            .fetch_all(&mut *transaction)
            .await?;
            query(
                "DELETE FROM shardline_oci_tags
                 WHERE scope_namespace = $1 AND repository = $2 AND digest_hex = $3",
            )
            .bind(&key.scope_namespace)
            .bind(&key.repository)
            .bind(&key.digest_hex)
            .execute(&mut *transaction)
            .await?;
            for row in tags {
                let scope_namespace: String = row.try_get("scope_namespace")?;
                let repository: String = row.try_get("repository")?;
                let tag: String = row.try_get("tag")?;
                let digest_hex: String = row.try_get("digest_hex")?;
                super::oci_tags::record_tag_transition(
                    transaction.as_mut(),
                    &scope_namespace,
                    &repository,
                    &tag,
                    Some(digest_hex),
                    None,
                )
                .await?;
            }
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
            verify_snapshot_evidence(&evidence, &expected)?;
        } else if let Some(head) = evidence.events().last() {
            // A new upload can persist its hidden state before immutable bytes
            // arrive. Reclaimed identities still have no active tombstone.
            let expected = oci_snapshot(key, head.after.state, head.after.deleted_at_unix_seconds)?;
            verify_snapshot_evidence(&evidence, &expected)?;
            if head.after.state == OciObjectLifecycleState::Deleted {
                transaction.commit().await?;
                return Ok(true);
            }
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
        let operation_ids = tombstones
            .iter()
            .map(|tombstone| -> Result<String, PostgresMetadataStoreError> {
                let identity = OciObjectIdentity::new(
                    tombstone.key.scope_namespace.clone(),
                    tombstone.key.repository.clone(),
                    tombstone.key.kind.as_str(),
                    tombstone.key.digest_hex.clone(),
                )?;
                Ok(OciObjectOperationId::new(&identity).into_string())
            })
            .collect::<Result<Vec<_>, PostgresMetadataStoreError>>()?;
        let heads = load_postgres_latest_evidence_heads(
            transaction.as_mut(),
            OperationKind::Visibility,
            &operation_ids,
        )
        .await?;
        for (tombstone, operation_id) in tombstones.iter().zip(operation_ids) {
            let event = heads.get(&operation_id).ok_or_else(|| {
                PostgresMetadataStoreError::Reliability(
                    shardline_reliability::ReliabilityError::OperationMismatch,
                )
            })?;
            let evidence = OciObjectEvidenceLog::from_head(serde_json::from_value(event.clone())?)?;
            let expected = oci_snapshot(
                &tombstone.key,
                OciObjectLifecycleState::Deleted,
                Some(tombstone.deleted_at_unix_seconds),
            )?;
            verify_snapshot_evidence(&evidence, &expected)?;
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
        super::super::connect_isolated_postgres().await
    }

    fn object(scope_namespace: &str) -> OciObjectKey {
        OciObjectKey {
            scope_namespace: scope_namespace.to_owned(),
            repository: "team/assets".to_owned(),
            kind: OciObjectKind::Manifest,
            digest_hex: "a".repeat(64),
        }
    }

    fn tag(scope_namespace: &str, name: &str, digest: char) -> OciTagEntry {
        OciTagEntry {
            scope_namespace: scope_namespace.to_owned(),
            repository: "team/assets".to_owned(),
            tag: name.to_owned(),
            digest_hex: digest.to_string().repeat(64),
        }
    }

    #[tokio::test]
    async fn owner_guard_upgrade_waits_for_released_mutation_barrier() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let upgrade_pool = pool
            .options()
            .clone()
            .max_connections(1)
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let upgrade_pid: i32 = query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&upgrade_pool)
            .await
            .unwrap();
        let mut writer = pool.acquire().await.unwrap();
        writer.close_on_drop();
        query("SELECT pg_advisory_lock_shared($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *writer)
            .await
            .unwrap();
        let upgrade = tokio::spawn(async move {
            PostgresIndexStore::new(upgrade_pool)
                .initialize_remote_delete_guard()
                .await
        });
        let mut waiting = false;
        for _ in 0..100 {
            waiting = query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks
                WHERE locktype='advisory' AND pid=$1 AND NOT granted AND mode='ExclusiveLock'
                AND objsubid=1 AND classid::bigint=(x'5348415244474301'::bit(64)::bigint >> 32)
                AND objid::bigint=(x'5348415244474301'::bit(64)::bigint & 4294967295))",
            )
            .bind(upgrade_pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            waiting,
            "upgrade must queue behind the existing writer barrier"
        );
        assert!(!upgrade.is_finished());
        query("SELECT pg_advisory_unlock_shared($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *writer)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), upgrade)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn ordinary_gc_inventory_is_owned_bounded_and_excludes_published_stages() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        query("ALTER TABLE shardline_reliability_events DISABLE TRIGGER shardline_oci_staged_tombstone_v1")
            .execute(&pool).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        connection.close_on_drop();
        for number in 0..66 {
            let key = OciObjectKey {
                digest_hex: format!("{number:064x}"),
                ..object("global")
            };
            store
                .stage_oci_object_on_connection(&mut connection, &key, &[])
                .await
                .unwrap();
            if number == 65 {
                store
                    .publish_oci_object_on_connection(&mut connection, &key, &[])
                    .await
                    .unwrap();
            }
        }
        // The cheap inventory prefilter also sees malformed legacy identities.
        // Full candidate parsing must retain their old ignored behavior.
        for (scope, repository) in [("global", ""), ("not-a-scope", "team/assets")] {
            let digest = "f".repeat(64);
            query("INSERT INTO shardline_reliability_events(operation_kind,operation_id,sequence,event_json,created_at_unix_seconds)
                VALUES('Visibility',$1,0,$2,1)")
                .bind(format!("{scope}:{repository}:manifest:{digest}"))
                .bind(serde_json::json!({"after":{"identity":{"scope_namespace":scope,
                    "repository":repository,"object_kind":"manifest","digest_hex":digest},
                    "state":"Deleted","deleted_at_unix_seconds":1}}))
                .execute(&mut *connection).await.unwrap();
        }
        assert!(
            store.restore_staged_oci_tombstones_for_gc().await.is_err(),
            "a separate unowned session cannot restore stages"
        );
        query("SELECT pg_advisory_lock($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *connection)
            .await
            .unwrap();
        assert!(
            query_scalar::<_, i64>(
                "SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),65)"
            )
            .fetch_one(&mut *connection)
            .await
            .is_err()
        );
        for expected in [64, 1, 0] {
            assert_eq!(
                query_scalar::<_, i64>(
                    "SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64)"
                )
                .fetch_one(&mut *connection)
                .await
                .unwrap(),
                expected
            );
        }
        let rows = store.list_oci_object_tombstones().await.unwrap();
        assert_eq!(rows.len(), 65);
        assert!(
            !rows
                .iter()
                .any(|row| row.key.digest_hex == format!("{:064x}", 65))
        );
        assert_eq!(store.list_oci_object_tombstones().await.unwrap(), rows);
    }

    #[tokio::test]
    async fn owner_upgrade_restores_only_current_hidden_manifest_generations() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        query("ALTER TABLE shardline_reliability_events DISABLE TRIGGER shardline_oci_staged_tombstone_v1").execute(&pool).await.unwrap();
        let hidden = object("global");
        let published = OciObjectKey {
            digest_hex: "b".repeat(64),
            ..hidden.clone()
        };
        let mut connection = pool.acquire().await.unwrap();
        store
            .stage_oci_object_on_connection(&mut connection, &hidden, &[])
            .await
            .unwrap();
        store
            .stage_oci_object_on_connection(&mut connection, &published, &[])
            .await
            .unwrap();
        store
            .publish_oci_object_on_connection(&mut connection, &published, &[])
            .await
            .unwrap();
        drop(connection);
        assert!(store.list_oci_object_tombstones().await.unwrap().is_empty());
        let physical = shardline_storage::ObjectKey::parse("owner-upgrade/staged-guard").unwrap();
        assert!(
            store
                .begin_remote_delete(&physical)
                .await
                .unwrap_err()
                .to_string()
                .contains("db migrate up")
        );
        store.initialize_remote_delete_guard().await.unwrap();
        let rows = store.list_oci_object_tombstones().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.first().unwrap().key, hidden);
        assert!(store.oci_object_is_deleted(&hidden).await.unwrap());
        assert!(!store.oci_object_is_deleted(&published).await.unwrap());
        let permit = store.begin_remote_delete(&physical).await.unwrap();
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        store.initialize_remote_delete_guard().await.unwrap();
        assert_eq!(store.list_oci_object_tombstones().await.unwrap(), rows);
        let operation = format!("global:team/assets:manifest:{}", hidden.digest_hex);
        let mut event: serde_json::Value = query_scalar("SELECT event_json FROM shardline_reliability_events WHERE operation_kind='Visibility' AND operation_id=$1 ORDER BY sequence DESC LIMIT 1")
            .bind(&operation).fetch_one(&pool).await.unwrap();
        *event
            .get_mut("after")
            .unwrap()
            .get_mut("identity")
            .unwrap()
            .get_mut("digest_hex")
            .unwrap() = serde_json::Value::String("f".repeat(64));
        query("UPDATE shardline_reliability_events SET event_json=$1 WHERE operation_kind='Visibility' AND operation_id=$2 AND sequence=(SELECT max(sequence) FROM shardline_reliability_events WHERE operation_kind='Visibility' AND operation_id=$2)")
            .bind(event).bind(&operation).execute(&pool).await.unwrap();
        assert!(store.list_oci_object_tombstones().await.is_err());
    }

    #[tokio::test]
    async fn staging_preserves_publication_and_needs_no_tombstone_insert_or_fence_grant() {
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let role = format!(
            "stage_oci_{}_{}",
            std::process::id(),
            schema.rsplit('_').next().unwrap()
        );
        query(sqlx::AssertSqlSafe(format!("CREATE ROLE {role}")))
            .execute(&pool)
            .await
            .unwrap();
        for sql in [
            format!("GRANT USAGE ON SCHEMA {schema} TO {role}"),
            format!("GRANT SELECT, DELETE ON {schema}.shardline_oci_object_tombstones TO {role}"),
            format!("GRANT SELECT ON {schema}.shardline_oci_tags TO {role}"),
            format!(
                "GRANT SELECT, INSERT, UPDATE ON {schema}.shardline_reliability_events TO {role}"
            ),
        ] {
            query(sqlx::AssertSqlSafe(sql))
                .execute(&pool)
                .await
                .unwrap();
        }
        let key = object("global");
        let mut actor = pool.acquire().await.unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role}"
        )))
        .execute(&mut *actor)
        .await
        .unwrap();
        let can_insert: bool = query_scalar(
            "SELECT has_table_privilege(current_user, 'shardline_oci_object_tombstones', 'INSERT')",
        )
        .fetch_one(&mut *actor)
        .await
        .unwrap();
        assert!(!can_insert);
        let can_read_fences: bool = query_scalar(
            "SELECT has_table_privilege(current_user, 'shardline_resource_fences', 'SELECT')",
        )
        .fetch_one(&mut *actor)
        .await
        .unwrap();
        assert!(!can_read_fences);
        store
            .stage_oci_object_on_connection(&mut actor, &key, &[])
            .await
            .unwrap();
        assert!(store.oci_object_is_deleted(&key).await.unwrap());
        let tombstones: i64 = query_scalar("SELECT count(*) FROM shardline_oci_object_tombstones")
            .fetch_one(&mut *actor)
            .await
            .unwrap();
        assert_eq!(tombstones, 1);
        store
            .publish_oci_object_on_connection(&mut actor, &key, &[])
            .await
            .unwrap();
        assert!(!store.oci_object_is_deleted(&key).await.unwrap());
        let before: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(&mut *actor)
            .await
            .unwrap();
        store
            .stage_oci_object_on_connection(&mut actor, &key, &[])
            .await
            .unwrap();
        let after: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(&mut *actor)
            .await
            .unwrap();
        assert_eq!(before, after);
        assert!(!store.oci_object_is_deleted(&key).await.unwrap());
        query("ALTER TABLE shardline_oci_object_tombstones ENABLE ROW LEVEL SECURITY")
            .execute(&pool)
            .await
            .unwrap();
        query(sqlx::AssertSqlSafe(format!("CREATE POLICY staging_scope ON shardline_oci_object_tombstones TO {role} USING (scope_namespace='global') WITH CHECK (scope_namespace='global')")))
            .execute(&pool).await.unwrap();
        let scoped = OciObjectKey {
            digest_hex: "c".repeat(64),
            ..key.clone()
        };
        store
            .stage_oci_object_on_connection(&mut actor, &scoped, &[])
            .await
            .unwrap();
        let rows: i64 = query_scalar("SELECT count(*) FROM shardline_oci_object_tombstones")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            rows, 0,
            "definer materialization must not bypass row policies"
        );
        assert!(store.oci_object_is_deleted(&scoped).await.unwrap());
        let backfill = include_str!("remote_delete_guard.sql")
            .split_once("DO $materialize_stages$")
            .unwrap()
            .1;
        let backfill = format!("DO $materialize_stages${backfill}")
            .replace("@SCHEMA@", &format!("\"{schema}\""))
            .replace("@SCHEMA_LITERAL@", &format!("'{schema}'"));
        sqlx::raw_sql(sqlx::AssertSqlSafe(backfill))
            .execute(&mut *actor)
            .await
            .unwrap();
        let backfilled_rows: i64 =
            query_scalar("SELECT count(*) FROM shardline_oci_object_tombstones")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            backfilled_rows, 0,
            "owner backfill also respects active row policies"
        );
        query("SELECT pg_advisory_lock($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *actor)
            .await
            .unwrap();
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64)"
            )
            .fetch_one(&mut *actor)
            .await
            .unwrap(),
            -1,
            "GC discovery must not bypass this account's row policies"
        );
        query("SELECT pg_advisory_unlock($1)")
            .bind(0x5348_4152_4447_4301_i64)
            .execute(&mut *actor)
            .await
            .unwrap();
        // The authorized owner can restore the candidate inventory without
        // changing the actor's grants or its existing publication operation.
        store.initialize_remote_delete_guard().await.unwrap();
        assert_eq!(store.list_oci_object_tombstones().await.unwrap().len(), 1);
        store
            .publish_oci_object_on_connection(&mut actor, &scoped, &[])
            .await
            .unwrap();
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *actor)
            .await
            .unwrap();
        drop(actor);
        query("DROP POLICY staging_scope ON shardline_oci_object_tombstones")
            .execute(&pool)
            .await
            .unwrap();
        query(sqlx::AssertSqlSafe(format!("DROP OWNED BY {role}")))
            .execute(&pool)
            .await
            .unwrap();
        query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pending_manifest_dependency_rolls_back_publication_and_tags() {
        use shardline_storage::ObjectKey;
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let repository_id = "378332dde522cf872eb553d009ee7448bfba2fe246d1913eb1fe2ba522ca5189";
        for (kind, part) in [
            (OciObjectKind::Blob, "blobs"),
            (OciObjectKind::Manifest, "manifests"),
            (OciObjectKind::Manifest, "manifest-media-types"),
        ] {
            let target = OciObjectKey {
                digest_hex: "b".repeat(64),
                ..object("global")
            };
            let dependency = OciObjectKey {
                kind,
                ..object("global")
            };
            store.publish_oci_object(&target, &[]).await.unwrap();
            store.delete_oci_object(&target).await.unwrap();
            let old_tag = tag("global", part, 'f');
            store.upsert_oci_tag(&old_tag).await.unwrap();
            let next_tag = OciTagEntry {
                digest_hex: target.digest_hex.clone(),
                ..old_tag.clone()
            };
            let physical = ObjectKey::parse(&format!(
                "protocols/oci/global/repos/{repository_id}/{part}/{}",
                dependency.digest_hex
            ))
            .unwrap();
            let permit = store.begin_remote_delete(&physical).await.unwrap();
            let before: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
                .fetch_one(&pool)
                .await
                .unwrap();
            let mut connection = pool.acquire().await.unwrap();
            let error = store
                .publish_oci_object_with_references_on_connection(
                    &mut connection,
                    &target,
                    std::slice::from_ref(&next_tag),
                    std::slice::from_ref(&dependency),
                )
                .await
                .unwrap_err();
            assert!(error.is_remote_delete_pending());
            assert!(store.oci_object_is_deleted(&target).await.unwrap());
            assert_eq!(
                store
                    .oci_tag("global", "team/assets", part)
                    .await
                    .unwrap()
                    .unwrap(),
                old_tag
            );
            let after: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(before, after);
            assert!(store.finish_remote_delete(&permit).await.unwrap());
            store
                .publish_oci_object_with_references_on_connection(
                    &mut connection,
                    &target,
                    std::slice::from_ref(&next_tag),
                    &[dependency],
                )
                .await
                .unwrap();
            assert!(!store.oci_object_is_deleted(&target).await.unwrap());
            assert_eq!(
                store
                    .oci_tag("global", "team/assets", part)
                    .await
                    .unwrap()
                    .unwrap(),
                next_tag
            );
        }
    }

    #[tokio::test]
    async fn pending_oci_delete_preserves_tombstones_tags_and_native_evidence() {
        use shardline_storage::ObjectKey;
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        // SHA-256 of "team/assets", matching the independent OCI adapter grammar.
        let repository_id = "378332dde522cf872eb553d009ee7448bfba2fe246d1913eb1fe2ba522ca5189";
        for (kind, part, digest) in [
            (OciObjectKind::Blob, "blobs", 'a'),
            (OciObjectKind::Manifest, "manifests", 'b'),
            (OciObjectKind::Manifest, "manifest-media-types", 'c'),
        ] {
            let key = OciObjectKey {
                kind,
                digest_hex: digest.to_string().repeat(64),
                ..object("global")
            };
            store.publish_oci_object(&key, &[]).await.unwrap();
            store.delete_oci_object(&key).await.unwrap();
            let stable = tag("global", part, 'f');
            store.upsert_oci_tag(&stable).await.unwrap();
            let physical = ObjectKey::parse(&format!(
                "protocols/oci/global/repos/{repository_id}/{part}/{}",
                key.digest_hex
            ))
            .unwrap();
            let permit = store.begin_remote_delete(&physical).await.unwrap();
            let before: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert!(
                store
                    .publish_oci_object(&key, &[])
                    .await
                    .unwrap_err()
                    .is_remote_delete_pending()
            );
            if kind == OciObjectKind::Manifest {
                let candidate = OciTagEntry {
                    digest_hex: key.digest_hex.clone(),
                    ..stable.clone()
                };
                assert!(
                    store
                        .upsert_oci_tag(&candidate)
                        .await
                        .unwrap_err()
                        .is_remote_delete_pending()
                );
                let error = query("UPDATE shardline_oci_tags SET digest_hex=$1 WHERE scope_namespace='global' AND repository='team/assets' AND tag=$2")
                    .bind(&key.digest_hex).bind(part).execute(&pool).await.unwrap_err();
                assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
            }
            let error = query("DELETE FROM shardline_oci_object_tombstones WHERE scope_namespace='global' AND repository='team/assets' AND object_kind=$1 AND digest_hex=$2")
                .bind(kind.as_str()).bind(&key.digest_hex).execute(&pool).await.unwrap_err();
            assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
            assert!(store.oci_object_is_deleted(&key).await.unwrap());
            let target: String = query_scalar("SELECT digest_hex FROM shardline_oci_tags WHERE scope_namespace='global' AND repository='team/assets' AND tag=$1")
                .bind(part).fetch_one(&pool).await.unwrap();
            assert_eq!(target, stable.digest_hex);
            let after: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(before, after);
            store
                .publish_oci_object(
                    &OciObjectKey {
                        scope_namespace: "ef".repeat(32),
                        ..key.clone()
                    },
                    &[],
                )
                .await
                .unwrap();
            store
                .publish_oci_object(
                    &OciObjectKey {
                        repository: "team/other".to_owned(),
                        ..key.clone()
                    },
                    &[],
                )
                .await
                .unwrap();
            assert!(store.finish_remote_delete(&permit).await.unwrap());
            store.publish_oci_object(&key, &[]).await.unwrap();
            assert!(!store.oci_object_is_deleted(&key).await.unwrap());
        }
        // No tombstone row or tag write is needed for a native publication to
        // check the physical blob. Its evidence insert must also roll back.
        let key = OciObjectKey {
            kind: OciObjectKind::Blob,
            digest_hex: "d".repeat(64),
            ..object(&"ef".repeat(32))
        };
        let physical = ObjectKey::parse(&format!(
            "protocols/oci/{}/repos/{repository_id}/blobs/{}",
            key.scope_namespace, key.digest_hex
        ))
        .unwrap();
        let permit = store.begin_remote_delete(&physical).await.unwrap();
        let before: i64 = query_scalar("SELECT count(*) FROM shardline_reliability_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            store
                .publish_oci_object(&key, &[])
                .await
                .unwrap_err()
                .is_remote_delete_pending()
        );
        assert_eq!(
            before,
            query_scalar::<_, i64>("SELECT count(*) FROM shardline_reliability_events")
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        store.publish_oci_object(&key, &[]).await.unwrap();
        pool.close().await;
    }

    #[tokio::test]
    async fn legacy_oci_column_writer_needs_no_fence_grants_and_gc_requires_guards() {
        use shardline_storage::ObjectKey;
        let Some(pool) = connect_postgres().await else {
            return;
        };
        let store = PostgresIndexStore::new(pool.clone());
        let key = object("global");
        let stable = tag("global", "stable", 'f');
        store.upsert_oci_tag(&stable).await.unwrap();
        store.publish_oci_object(&key, &[]).await.unwrap();
        store.delete_oci_object(&key).await.unwrap();
        let physical = ObjectKey::parse(&format!("protocols/oci/global/repos/378332dde522cf872eb553d009ee7448bfba2fe246d1913eb1fe2ba522ca5189/manifests/{}", key.digest_hex)).unwrap();
        let permit = store.begin_remote_delete(&physical).await.unwrap();
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let role = format!(
            "oci_column_writer_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role}; GRANT USAGE ON SCHEMA {schema} TO {role};
             GRANT SELECT, UPDATE(digest_hex) ON {schema}.shardline_oci_tags TO {role};
             GRANT SELECT ON {schema}.shardline_reliability_events TO {role};
             GRANT SELECT, DELETE ON {schema}.shardline_oci_object_tombstones TO {role};"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let mut actor = pool.acquire().await.unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role}"
        )))
        .execute(&mut *actor)
        .await
        .unwrap();
        assert!(
            query("SELECT * FROM shardline_resource_fences")
                .fetch_all(&mut *actor)
                .await
                .is_err()
        );
        let error = query("UPDATE shardline_oci_tags SET digest_hex=$1 WHERE tag='stable'")
            .bind(&key.digest_hex)
            .execute(&mut *actor)
            .await
            .unwrap_err();
        assert!(PostgresMetadataStoreError::from(error).is_remote_delete_pending());
        let tombstone_error =
            query("DELETE FROM shardline_oci_object_tombstones WHERE digest_hex=$1")
                .bind(&key.digest_hex)
                .execute(&mut *actor)
                .await
                .unwrap_err();
        assert!(PostgresMetadataStoreError::from(tombstone_error).is_remote_delete_pending());
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *actor)
            .await
            .unwrap();
        drop(actor);
        assert!(store.finish_remote_delete(&permit).await.unwrap());
        // Existing tag publication still requires its original matching journal.
        // Seed that through the normal API before the legacy column-only update.
        store
            .upsert_oci_tag(&OciTagEntry {
                digest_hex: key.digest_hex.clone(),
                ..stable
            })
            .await
            .unwrap();
        let mut reconciled_actor = pool.acquire().await.unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "SET SESSION AUTHORIZATION {role}"
        )))
        .execute(&mut *reconciled_actor)
        .await
        .unwrap();
        query("UPDATE shardline_oci_tags SET digest_hex=$1 WHERE tag='stable'")
            .bind(&key.digest_hex)
            .execute(&mut *reconciled_actor)
            .await
            .unwrap();
        query("DELETE FROM shardline_oci_object_tombstones WHERE digest_hex=$1")
            .bind(&key.digest_hex)
            .execute(&mut *reconciled_actor)
            .await
            .unwrap();
        query("RESET SESSION AUTHORIZATION")
            .execute(&mut *reconciled_actor)
            .await
            .unwrap();
        drop(reconciled_actor);
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {role}; DROP ROLE {role}"
        )))
        .execute(&pool)
        .await
        .unwrap();
        query("ALTER TABLE shardline_oci_tags DISABLE TRIGGER shardline_oci_reference_remote_delete_v1").execute(&pool).await.unwrap();
        assert!(
            store
                .begin_remote_delete(&physical)
                .await
                .unwrap_err()
                .to_string()
                .contains("db migrate up")
        );
        store.initialize_remote_delete_guard().await.unwrap();
        let upgraded_permit = store.begin_remote_delete(&physical).await.unwrap();
        assert!(store.finish_remote_delete(&upgraded_permit).await.unwrap());
        pool.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pg_manifest_tombstone_and_tags_share_one_commit() {
        let Some(pool) = connect_postgres().await else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let scope_namespace = format!(
            "oci-tombstone-pg-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        query("DELETE FROM shardline_oci_object_tombstones WHERE scope_namespace = $1")
            .bind(&scope_namespace)
            .execute(&pool)
            .await
            .unwrap();
        query("DELETE FROM shardline_oci_tags WHERE scope_namespace = $1")
            .bind(&scope_namespace)
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
            scope_namespace,
            "team/assets",
            OciObjectKind::Manifest.as_str(),
            "a".repeat(64)
        ))
        .execute(&pool)
        .await
        .unwrap();
        let store = PostgresIndexStore::new(pool);
        let manifest = object(&scope_namespace);
        let current = tag(&scope_namespace, "latest", 'a');
        let unrelated = tag(&scope_namespace, "stable", 'b');

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
