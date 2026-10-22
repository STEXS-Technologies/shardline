//! Keep a listing's session rows, parts and evidence in one committed snapshot.
use sqlx::{PgPool, Postgres, Transaction};

pub(super) async fn begin(pool: &PgPool) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    // READ COMMITTED can pair an earlier session row with a later evidence
    // head. A bounded read-only snapshot avoids that race without row locks
    // or row-lock WAL on the listing path.
    pool.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::super::{session_from_row, verify_resumable_session_page};
    use sqlx::Row;
    use std::time::Duration;

    #[tokio::test]
    async fn postgres_listing_snapshot_survives_committed_completion_without_locking_writer() {
        let Some(store) = super::super::tests::store().await else {
            eprintln!("skipping: no reachable DATABASE_URL");
            return;
        };
        let expiry =
            Duration::from_secs(u64::try_from(chrono::Utc::now().timestamp()).unwrap() + 3600);
        let active = super::super::tests::session("listing-snapshot", expiry);
        assert!(store.create_resumable_session(&active).await.unwrap());
        let mut snapshot = super::begin(store.pool()).await.unwrap();
        let row = sqlx::query(
            "SELECT session_id, protocol, scope_namespace, target_key, attributes_json,
                    state, generation, fence_epoch, expires_at, state_digest
             FROM shardline_resumable_sessions WHERE session_id = $1",
        )
        .bind(active.session_id())
        .fetch_one(snapshot.as_mut())
        .await
        .unwrap();
        let observed = session_from_row(&row).unwrap();
        let digest = row.try_get::<Option<String>, _>("state_digest").unwrap();
        // Commit between the listing's session query and its evidence queries.
        // A read-only listing must not block this writer or mix the two states.
        tokio::time::timeout(
            Duration::from_secs(5),
            store.begin_resumable_completion(active.session_id()),
        )
        .await
        .expect("listing must not hold a writer-blocking row lock")
        .unwrap()
        .unwrap();
        verify_resumable_session_page(&mut snapshot, &[observed], &[digest])
            .await
            .expect("a committed transition must not invalidate an earlier listing snapshot");
        snapshot.commit().await.unwrap();
        let listed = store
            .list_resumable_sessions(None, None, Some(active.session_id()), 2)
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed.first().unwrap().state(),
            crate::ResumableSessionState::Completing
        );
        let scoped = store
            .list_scoped_inflight_resumable_sessions(
                active.scope_namespace(),
                active.protocol(),
                None,
                2,
            )
            .await
            .unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped.first().unwrap().session_id(), active.session_id());
        // A new snapshot must still reject corrupt authoritative metadata.
        sqlx::query("UPDATE shardline_resumable_sessions SET state_digest = 'corrupt' WHERE session_id = $1")
            .bind(active.session_id())
            .execute(store.pool())
            .await
            .unwrap();
        assert!(
            store
                .list_resumable_sessions(None, None, Some(active.session_id()), 2)
                .await
                .is_err()
        );
        assert!(
            store
                .list_scoped_inflight_resumable_sessions(
                    active.scope_namespace(),
                    active.protocol(),
                    None,
                    2,
                )
                .await
                .is_err()
        );
    }
}
