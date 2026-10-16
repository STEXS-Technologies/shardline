//! Upload progress committed on a caller-owned, non-replaceable connection.
use super::{
    PostgresIndexStore, PostgresMetadataStoreError,
    index_store::{insert_reliability_event, load_postgres_latest_evidence_event},
};
use crate::UploadIntentState;
use shardline_reliability::{LifecycleEvent, OperationKind, verify_upload_lifecycle_head};
use sqlx::{Connection, PgConnection, Row};

impl PostgresIndexStore {
    /// Creates or verifies an upload intent using the caller's owned connection.
    /// Existing idempotency and legacy evidence migration rules are preserved.
    ///
    /// # Errors
    /// Returns invalid identity, evidence corruption or database/connection failure.
    pub async fn create_upload_intent_scoped_on_connection(
        connection: &mut PgConnection,
        intent: &crate::UploadIntent,
        tenant: &str,
        repository: &str,
    ) -> Result<(), PostgresMetadataStoreError> {
        let length = super::u64_to_i64(intent.object_length())?;
        let event = shardline_reliability::upload_lifecycle_event(
            tenant,
            repository,
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Created,
        )?;
        let transaction = connection.begin().await?;
        super::index_store::create_upload_intent_in_transaction(
            transaction,
            intent,
            tenant,
            repository,
            length,
            event,
        )
        .await
    }

    /// Reads verified upload progress without replacing the caller's connection.
    /// The row lock keeps the intent and its evidence head consistent during this read.
    ///
    /// # Errors
    /// Returns invalid stored state/evidence or database/connection failure.
    pub async fn upload_intent_by_id_on_connection(
        connection: &mut PgConnection,
        intent_id: &str,
    ) -> Result<Option<crate::UploadIntent>, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let row = sqlx::query("SELECT object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE intent_id=$1 FOR SHARE")
            .bind(intent_id).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let text: String = row.try_get("state")?;
        let state = UploadIntentState::parse(&text)
            .ok_or(PostgresMetadataStoreError::InvalidUploadIntentState(text))?;
        let intent = crate::UploadIntent::from_parts(
            intent_id.to_owned(),
            row.try_get("object_key")?,
            row.try_get("object_hash")?,
            super::i64_to_u64(row.try_get("object_length")?)?,
            state,
            std::time::Duration::from_secs(super::i64_to_u64(
                row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")?
                    .timestamp(),
            )?),
            std::time::Duration::from_secs(super::i64_to_u64(
                row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")?
                    .timestamp(),
            )?),
        );
        let head = load_postgres_latest_evidence_event(
            &mut *transaction,
            OperationKind::Upload,
            intent_id,
        )
        .await?
        .ok_or(PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::OperationMismatch,
        ))?;
        let head: LifecycleEvent = serde_json::from_value(head)?;
        verify_upload_lifecycle_head(
            &head,
            &head.operation.tenant,
            &head.operation.repository,
            intent_id,
            intent.object_key(),
            intent.object_hash(),
            state,
        )?;
        transaction.commit().await?;
        Ok(Some(intent))
    }

    /// Commits one upload transition and its evidence on the caller's connection.
    ///
    /// The caller retains responsibility for acquiring request ownership on this
    /// connection. This method never opens a pool connection or replaces a lost
    /// session. A completed transaction does not establish object-store presence.
    ///
    /// # Errors
    /// Returns connection loss, invalid stored evidence or invalid event identity.
    pub async fn transition_upload_intent_with_event_on_connection(
        connection: &mut PgConnection,
        intent_id: &str,
        next: UploadIntentState,
        event: &LifecycleEvent,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let mut transaction = connection.begin().await?;
        let row = sqlx::query("SELECT object_key, object_hash, object_length, state, created_at, updated_at FROM shardline_upload_intents WHERE intent_id=$1 FOR UPDATE")
            .bind(intent_id).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let text: String = row.try_get("state")?;
        let state = UploadIntentState::parse(&text)
            .ok_or(PostgresMetadataStoreError::InvalidUploadIntentState(text))?;
        let key: String = row.try_get("object_key")?;
        let hash: String = row.try_get("object_hash")?;
        // Retain the compatibility reader's validation of numeric metadata.
        super::i64_to_u64(row.try_get("object_length")?)?;
        for field in ["created_at", "updated_at"] {
            super::i64_to_u64(
                row.try_get::<chrono::DateTime<chrono::Utc>, _>(field)?
                    .timestamp(),
            )?;
        }
        let head = load_postgres_latest_evidence_event(
            &mut *transaction,
            OperationKind::Upload,
            intent_id,
        )
        .await?
        .ok_or(PostgresMetadataStoreError::Reliability(
            shardline_reliability::ReliabilityError::OperationMismatch,
        ))?;
        let head: LifecycleEvent = serde_json::from_value(head)?;
        verify_upload_lifecycle_head(
            &head,
            &head.operation.tenant,
            &head.operation.repository,
            intent_id,
            &key,
            &hash,
            state,
        )?;
        if state == next {
            transaction.rollback().await?;
            return Ok(true);
        }
        if !state.can_transition_to(next) {
            transaction.rollback().await?;
            return Ok(false);
        }
        event.validate_for_transition(intent_id, state, next)?;
        verify_upload_lifecycle_head(
            event,
            &head.operation.tenant,
            &head.operation.repository,
            intent_id,
            &key,
            &hash,
            next,
        )?;
        let previous = super::upload_recovery_guard::enter(
            &mut transaction,
            intent_id,
            &head.operation.repository,
        )
        .await?;
        let rows = sqlx::query(
            "UPDATE shardline_upload_intents SET state=$1, updated_at=now() WHERE intent_id=$2",
        )
        .bind(next.as_str())
        .bind(intent_id)
        .execute(&mut *transaction)
        .await?;
        if rows.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }
        insert_reliability_event(&mut transaction, event).await?;
        super::upload_recovery_guard::leave(&mut transaction, previous.as_deref()).await?;
        transaction.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{UploadIntent, UploadIntentStore};

    #[tokio::test]
    async fn upload_writer_marker_does_not_escape_a_nested_transaction() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            eprintln!("SKIPPED: nested upload writer marker requires PostgreSQL");
            return;
        };
        let index = PostgresIndexStore::new(pool.clone());
        let intent = UploadIntent::new(
            "file-v2-nested-marker".to_owned(),
            "record/nested-marker".to_owned(),
            "00".repeat(32),
            0,
        );
        index.create_intent(&intent).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        let mut outer = connection.begin().await.unwrap();
        sqlx::query("SELECT set_config('shardline.upload_intent_writer_v2', 'outer-marker', true)")
            .execute(&mut *outer)
            .await
            .unwrap();
        let event = shardline_reliability::upload_lifecycle_event(
            "shardline",
            "default",
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            UploadIntentState::Created,
            UploadIntentState::Storing,
        )
        .unwrap();
        assert!(
            PostgresIndexStore::transition_upload_intent_with_event_on_connection(
                &mut outer,
                intent.intent_id(),
                UploadIntentState::Storing,
                &event
            )
            .await
            .unwrap()
        );
        let restored_marker: String =
            sqlx::query_scalar("SELECT current_setting('shardline.upload_intent_writer_v2', true)")
                .fetch_one(&mut *outer)
                .await
                .unwrap();
        assert_eq!(restored_marker, "outer-marker");
        outer.rollback().await.unwrap();
        let rolled_back_marker: String = sqlx::query_scalar(
            "SELECT COALESCE(current_setting('shardline.upload_intent_writer_v2', true), '')",
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap();
        assert_eq!(rolled_back_marker, "");
        let updated =
            sqlx::query("UPDATE shardline_upload_intents SET state='Storing' WHERE intent_id=$1")
                .bind(intent.intent_id())
                .execute(&mut *connection)
                .await
                .unwrap();
        assert_eq!(updated.rows_affected(), 0);
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Created
        );
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn caller_connection_loss_cannot_commit_an_upload_transition() {
        let Some(pool) = super::super::connect_isolated_postgres().await else {
            eprintln!("SKIPPED: connection-bound upload transition requires PostgreSQL");
            return;
        };
        let index = PostgresIndexStore::new(pool.clone());
        let intent = UploadIntent::new(
            "connection-bound-upload".to_owned(),
            "xorbs/owned".to_owned(),
            "00".repeat(32),
            4,
        );
        let mut owner = pool.acquire().await.unwrap();
        owner.close_on_drop();
        PostgresIndexStore::create_upload_intent_scoped_on_connection(
            &mut owner,
            &intent,
            "shardline",
            "default",
        )
        .await
        .unwrap();
        let baseline = index.reliability_events(intent.intent_id()).await.unwrap();
        PostgresIndexStore::create_upload_intent_scoped_on_connection(
            &mut owner,
            &intent,
            "shardline",
            "default",
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(index.reliability_events(intent.intent_id()).await.unwrap())
                .unwrap(),
            serde_json::to_value(baseline).unwrap()
        );
        assert_eq!(
            PostgresIndexStore::upload_intent_by_id_on_connection(&mut owner, intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Created
        );
        let event = |from, to| {
            shardline_reliability::upload_lifecycle_event(
                "shardline",
                "default",
                intent.intent_id(),
                intent.object_key(),
                intent.object_hash(),
                from,
                to,
            )
            .unwrap()
        };
        let unrelated = shardline_reliability::upload_lifecycle_event(
            "shardline",
            "default",
            intent.intent_id(),
            "xorbs/unrelated",
            intent.object_hash(),
            shardline_reliability::UploadLifecycleState::Created,
            shardline_reliability::UploadLifecycleState::Storing,
        )
        .unwrap();
        assert!(
            PostgresIndexStore::transition_upload_intent_with_event_on_connection(
                &mut owner,
                intent.intent_id(),
                UploadIntentState::Storing,
                &unrelated
            )
            .await
            .is_err()
        );
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Created
        );
        assert!(
            PostgresIndexStore::transition_upload_intent_with_event_on_connection(
                &mut owner,
                intent.intent_id(),
                UploadIntentState::Storing,
                &event(
                    shardline_reliability::UploadLifecycleState::Created,
                    shardline_reliability::UploadLifecycleState::Storing
                )
            )
            .await
            .unwrap()
        );
        let before = index.reliability_events(intent.intent_id()).await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *owner)
            .await
            .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        assert!(
            PostgresIndexStore::transition_upload_intent_with_event_on_connection(
                &mut owner,
                intent.intent_id(),
                UploadIntentState::Stored,
                &event(
                    shardline_reliability::UploadLifecycleState::Storing,
                    shardline_reliability::UploadLifecycleState::Stored
                )
            )
            .await
            .is_err()
        );
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
        assert_eq!(
            serde_json::to_value(index.reliability_events(intent.intent_id()).await.unwrap())
                .unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }
}
