//! Observe the second ref inside the real uncommitted publication transaction.
use super::*;

pub(super) struct Publication {
    pool: sqlx::PgPool,
    blocker: sqlx::Transaction<'static, sqlx::Postgres>,
    refs: Vec<(String, String)>,
    revisions: i64,
    evidence: i64,
}

impl Publication {
    pub(super) async fn arm(database: &str) -> Self {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(10))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '45s'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(database)
            .await
            .unwrap();
        let refs = sqlx::query_as("SELECT ref_name, sha FROM shardline_hub_refs WHERE repo_id = 'drill/drill' ORDER BY ref_name")
            .fetch_all(&pool).await.unwrap();
        let revisions = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_hub_revisions")
            .fetch_one(&pool)
            .await
            .unwrap();
        let evidence = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_reliability_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(174893, 1)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        // This entire database is owned by this test. Before stopping at peer,
        // prove the same transaction has already updated main to the new head.
        sqlx::raw_sql("CREATE FUNCTION public.git_process_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF (SELECT sha FROM shardline_hub_refs WHERE repo_id = NEW.repo_id AND ref_name = 'main') IS DISTINCT FROM NEW.sha THEN
                RAISE EXCEPTION 'main was not updated before peer';
            END IF;
            PERFORM pg_advisory_xact_lock(174893, 1);
            RETURN NEW;
          END $$;
          CREATE TRIGGER git_process_cut BEFORE INSERT ON shardline_hub_refs
          FOR EACH ROW WHEN (NEW.repo_id = 'drill/drill' AND NEW.ref_name = 'peer')
          EXECUTE FUNCTION public.git_process_cut();")
            .execute(&pool).await.unwrap();
        Self {
            pool,
            blocker,
            refs,
            revisions,
            evidence,
        }
    }

    pub(super) async fn observe(&self) {
        for _ in 0..250 {
            let reached: bool = sqlx::query_scalar("SELECT EXISTS (
                SELECT 1 FROM pg_locks locks JOIN pg_stat_activity activity ON activity.pid = locks.pid
                WHERE locks.locktype = 'advisory' AND locks.classid = 174893
                AND locks.objid = 1 AND locks.objsubid = 2 AND NOT locks.granted)")
                .fetch_one(&self.pool).await.unwrap();
            if reached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("Git publication never reached its second-ref transaction boundary");
    }

    pub(super) async fn verify_rollback(self) {
        self.blocker.rollback().await.unwrap();
        // DDL waits for the killed process's transaction to finish rollback.
        sqlx::raw_sql("DROP TRIGGER git_process_cut ON shardline_hub_refs; DROP FUNCTION public.git_process_cut();")
            .execute(&self.pool).await.unwrap();
        let refs: Vec<(String, String)> = sqlx::query_as("SELECT ref_name, sha FROM shardline_hub_refs WHERE repo_id = 'drill/drill' ORDER BY ref_name")
            .fetch_all(&self.pool).await.unwrap();
        let revisions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_hub_revisions")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let evidence: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_reliability_events")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        assert_eq!(refs, self.refs, "interrupted batch changed durable refs");
        assert_eq!(
            revisions, self.revisions,
            "interrupted batch published revisions"
        );
        assert_eq!(
            evidence, self.evidence,
            "interrupted batch published evidence"
        );
        self.pool.close().await;
    }
}
