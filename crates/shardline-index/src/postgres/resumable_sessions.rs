use std::{num::NonZeroU64, time::Duration};

use sqlx::{Postgres, Row, Transaction};

use super::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64, u64_to_i64};
use crate::{
    ResumableSession, ResumableSessionError, ResumableSessionPart, ResumableSessionProtocol,
    ResumableSessionState,
};

fn session_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ResumableSession, PostgresMetadataStoreError> {
    let protocol_text: String = row.try_get("protocol")?;
    let protocol = ResumableSessionProtocol::parse(&protocol_text)
        .ok_or(ResumableSessionError::UnknownProtocol(protocol_text))?;
    let state_text: String = row.try_get("state")?;
    let state = ResumableSessionState::parse(&state_text)
        .ok_or(ResumableSessionError::UnknownState(state_text))?;
    let expires_at: chrono::DateTime<chrono::Utc> = row.try_get("expires_at")?;
    let expires_at_seconds = u64::try_from(expires_at.timestamp())
        .map_err(|error| PostgresMetadataStoreError::IntegerOutOfRange(error.to_string()))?;
    Ok(ResumableSession::from_parts(
        row.try_get("session_id")?,
        protocol,
        row.try_get("scope_namespace")?,
        row.try_get("target_key")?,
        state,
        i64_to_u64(row.try_get("generation")?)?,
        i64_to_u64(row.try_get("fence_epoch")?)?,
        Duration::from_secs(expires_at_seconds),
    )?)
}

async fn parts_on_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: &str,
) -> Result<Vec<ResumableSessionPart>, PostgresMetadataStoreError> {
    let rows = sqlx::query(
        "SELECT part_number, generation, staging_key, size_bytes, etag
         FROM shardline_resumable_session_parts
         WHERE session_id = $1
         ORDER BY part_number",
    )
    .bind(session_id)
    .fetch_all(&mut **transaction)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ResumableSessionPart::new(
                NonZeroU64::new(i64_to_u64(row.try_get("part_number")?)?)
                    .ok_or(ResumableSessionError::ZeroGeneration)?,
                NonZeroU64::new(i64_to_u64(row.try_get("generation")?)?)
                    .ok_or(ResumableSessionError::ZeroGeneration)?,
                row.try_get("staging_key")?,
                i64_to_u64(row.try_get("size_bytes")?)?,
                row.try_get("etag")?,
            ))
        })
        .collect()
}

impl PostgresIndexStore {
    /// Idempotently creates a durable resumable session with immutable identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the expiry cannot be represented or Postgres rejects the write.
    pub async fn create_resumable_session(
        &self,
        session: &ResumableSession,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let expires_at = chrono::DateTime::<chrono::Utc>::from_timestamp(
            i64::try_from(session.expires_at().as_secs()).map_err(|error| {
                PostgresMetadataStoreError::IntegerOutOfRange(error.to_string())
            })?,
            0,
        )
        .ok_or_else(|| PostgresMetadataStoreError::IntegerOutOfRange("invalid expiry".into()))?;
        let result = sqlx::query(
            "INSERT INTO shardline_resumable_sessions (
                 session_id, protocol, scope_namespace, target_key, state,
                 generation, fence_epoch, expires_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (session_id) DO NOTHING",
        )
        .bind(session.session_id())
        .bind(session.protocol().as_str())
        .bind(session.scope_namespace())
        .bind(session.target_key())
        .bind(session.state().as_str())
        .bind(u64_to_i64(session.generation().get())?)
        .bind(u64_to_i64(session.fence_epoch().get())?)
        .bind(expires_at)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Loads a resumable session by opaque ID.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres fails or durable data violates the typed contract.
    pub async fn resumable_session_by_id(
        &self,
        session_id: &str,
    ) -> Result<Option<ResumableSession>, PostgresMetadataStoreError> {
        sqlx::query(
            "SELECT session_id, protocol, scope_namespace, target_key, state,
                    generation, fence_epoch, expires_at
             FROM shardline_resumable_sessions WHERE session_id = $1",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?
        .as_ref()
        .map(session_from_row)
        .transpose()
    }

    /// Atomically selects one immutable staged object as the current part.
    ///
    /// The object must already exist. Publication succeeds only while the
    /// session is active and unexpired according to the database clock.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres rejects the transactional publication.
    pub async fn publish_resumable_part(
        &self,
        session_id: &str,
        part_number: NonZeroU64,
        staging_key: &str,
        size_bytes: u64,
        etag: Option<&str>,
    ) -> Result<Option<ResumableSessionPart>, PostgresMetadataStoreError> {
        let mut transaction = self.pool().begin().await?;
        let generation: Option<i64> = sqlx::query_scalar(
            "UPDATE shardline_resumable_sessions
             SET generation = generation + 1, updated_at = now()
             WHERE session_id = $1 AND state = 'active' AND expires_at > clock_timestamp()
             RETURNING generation",
        )
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(generation) = generation else {
            transaction.rollback().await?;
            return Ok(None);
        };
        sqlx::query(
            "INSERT INTO shardline_resumable_session_parts (
                 session_id, part_number, generation, staging_key, size_bytes, etag
             ) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (session_id, part_number) DO UPDATE SET
                 generation = EXCLUDED.generation,
                 staging_key = EXCLUDED.staging_key,
                 size_bytes = EXCLUDED.size_bytes,
                 etag = EXCLUDED.etag",
        )
        .bind(session_id)
        .bind(u64_to_i64(part_number.get())?)
        .bind(generation)
        .bind(staging_key)
        .bind(u64_to_i64(size_bytes)?)
        .bind(etag)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(ResumableSessionPart::new(
            part_number,
            NonZeroU64::new(i64_to_u64(generation)?)
                .ok_or(ResumableSessionError::ZeroGeneration)?,
            staging_key.to_owned(),
            size_bytes,
            etag.map(str::to_owned),
        )))
    }

    /// Pins the current part map and moves an active session to `completing`.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres cannot atomically pin the session and part map.
    pub async fn begin_resumable_completion(
        &self,
        session_id: &str,
    ) -> Result<Option<(ResumableSession, Vec<ResumableSessionPart>)>, PostgresMetadataStoreError>
    {
        let mut transaction = self.pool().begin().await?;
        let row = sqlx::query(
            "UPDATE shardline_resumable_sessions
             SET state = 'completing', generation = generation + 1,
                 fence_epoch = fence_epoch + 1, updated_at = now()
             WHERE session_id = $1 AND state = 'active' AND expires_at > clock_timestamp()
             RETURNING session_id, protocol, scope_namespace, target_key, state,
                       generation, fence_epoch, expires_at",
        )
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let session = session_from_row(&row)?;
        let parts = parts_on_transaction(&mut transaction, session_id).await?;
        transaction.commit().await?;
        Ok(Some((session, parts)))
    }

    /// Compare-and-set transition using the completion owner's fence epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when Postgres rejects the conditional transition.
    pub async fn transition_resumable_session(
        &self,
        session_id: &str,
        expected_state: ResumableSessionState,
        expected_fence_epoch: NonZeroU64,
        next_state: ResumableSessionState,
    ) -> Result<bool, PostgresMetadataStoreError> {
        if !expected_state.can_transition_to(next_state) {
            return Ok(false);
        }
        let result = sqlx::query(
            "UPDATE shardline_resumable_sessions
             SET state = $1, updated_at = now()
             WHERE session_id = $2 AND state = $3 AND fence_epoch = $4",
        )
        .bind(next_state.as_str())
        .bind(session_id)
        .bind(expected_state.as_str())
        .bind(u64_to_i64(expected_fence_epoch.get())?)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Atomically expires up to `limit` active sessions using the database clock.
    ///
    /// Returned IDs own no authoritative protocol state after this transaction;
    /// their staging objects may be reclaimed asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error when the limit cannot be represented or Postgres rejects the update.
    pub async fn expire_resumable_sessions(
        &self,
        limit: usize,
    ) -> Result<Vec<String>, PostgresMetadataStoreError> {
        let limit = i64::try_from(limit)
            .map_err(|error| PostgresMetadataStoreError::IntegerOutOfRange(error.to_string()))?;
        let rows = sqlx::query(
            "WITH expired AS (
                 SELECT session_id FROM shardline_resumable_sessions
                 WHERE state = 'active' AND expires_at <= clock_timestamp()
                 ORDER BY expires_at, session_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT $1
             )
             UPDATE shardline_resumable_sessions AS sessions
             SET state = 'expired', generation = generation + 1,
                 fence_epoch = fence_epoch + 1, updated_at = now()
             FROM expired
             WHERE sessions.session_id = expired.session_id
             RETURNING sessions.session_id",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| row.try_get("session_id").map_err(Into::into))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    async fn store() -> Option<PostgresIndexStore> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .ok()?;
        sqlx::raw_sql(include_str!(
            "../../../../migrations/20260823000000_resumable_sessions.up.sql"
        ))
        .execute(&pool)
        .await
        .ok()?;
        Some(PostgresIndexStore::new(pool))
    }

    fn session(prefix: &str, expires_at: Duration) -> ResumableSession {
        let suffix = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        ResumableSession::new(
            format!("{prefix}-{suffix}"),
            ResumableSessionProtocol::S3Multipart,
            "owner/repository".to_owned(),
            "large/model.bin".to_owned(),
            expires_at,
        )
    }

    #[tokio::test]
    async fn postgres_parts_are_pinned_and_completion_is_fenced() {
        let Some(store) = store().await else {
            eprintln!("skipping: no reachable DATABASE_URL");
            return;
        };
        let expiry = Duration::from_secs(
            u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default() + 3600,
        );
        let session = session("parts", expiry);
        assert!(store.create_resumable_session(&session).await.unwrap());

        let first = store.clone();
        let second = store.clone();
        let session_id = session.session_id().to_owned();
        let first_id = session_id.clone();
        let first_publish = tokio::spawn(async move {
            first
                .publish_resumable_part(
                    &first_id,
                    NonZeroU64::new(1).unwrap(),
                    "staging/first",
                    5,
                    Some("e1"),
                )
                .await
        });
        let second_publish = tokio::spawn(async move {
            second
                .publish_resumable_part(
                    &session_id,
                    NonZeroU64::new(2).unwrap(),
                    "staging/second",
                    7,
                    Some("e2"),
                )
                .await
        });
        assert!(first_publish.await.unwrap().unwrap().is_some());
        assert!(second_publish.await.unwrap().unwrap().is_some());

        let (claimed, parts) = store
            .begin_resumable_completion(session.session_id())
            .await
            .unwrap()
            .expect("active session must be claimable");
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts.first().map(ResumableSessionPart::part_number),
            NonZeroU64::new(1)
        );
        assert_eq!(
            parts.get(1).map(ResumableSessionPart::part_number),
            NonZeroU64::new(2)
        );
        assert!(
            store
                .publish_resumable_part(
                    session.session_id(),
                    NonZeroU64::new(3).unwrap(),
                    "staging/late",
                    1,
                    None
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !store
                .transition_resumable_session(
                    session.session_id(),
                    ResumableSessionState::Completing,
                    NonZeroU64::new(claimed.fence_epoch().get() + 1).unwrap(),
                    ResumableSessionState::Completed,
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .transition_resumable_session(
                    session.session_id(),
                    ResumableSessionState::Completing,
                    claimed.fence_epoch(),
                    ResumableSessionState::Completed,
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn postgres_expiry_uses_database_time_and_blocks_late_parts() {
        let Some(store) = store().await else {
            eprintln!("skipping: no reachable DATABASE_URL");
            return;
        };
        let session = session("expired", Duration::from_secs(1));
        assert!(store.create_resumable_session(&session).await.unwrap());
        let expired = store.expire_resumable_sessions(100).await.unwrap();
        assert!(expired.iter().any(|id| id == session.session_id()));
        assert!(
            store
                .publish_resumable_part(
                    session.session_id(),
                    NonZeroU64::MIN,
                    "staging/late",
                    1,
                    None
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .resumable_session_by_id(session.session_id())
                .await
                .unwrap()
                .expect("session remains as a durable tombstone")
                .state(),
            ResumableSessionState::Expired
        );
    }
}
