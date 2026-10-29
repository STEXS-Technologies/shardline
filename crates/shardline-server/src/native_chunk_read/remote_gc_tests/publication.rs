//! Real publication must queue behind destructive ownership, including index repair.
use crate::{PostgresBackend, ServerObjectStore};
use std::{num::NonZeroUsize, time::Duration};

const FILE: &str = "publication-queued-behind-native-repair";
const PAYLOAD: &[u8] = b"acknowledged publication after remote GC and member repair";

pub(super) struct Pending {
    _root: tempfile::TempDir,
    backend: PostgresBackend,
    worker: tokio::task::JoinHandle<Result<(), &'static str>>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Pending {
    pub(super) async fn start(
        database: &str,
        objects: ServerObjectStore,
        observer: &sqlx::PgPool,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let backend = PostgresBackend::new_with_object_store(
            root.path().to_owned(),
            "http://localhost".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            database,
            objects,
        )
        .await
        .unwrap()
        .with_single_connection_pools(database)
        .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&backend.gc_barrier_pool)
            .await
            .unwrap();
        let writer = backend.clone();
        let pending = Self {
            _root: root,
            backend,
            worker: tokio::spawn(async move {
                writer
                    .upload_file(FILE, bytes::Bytes::from_static(PAYLOAD), None)
                    .await
                    .map(|_| ())
                    .map_err(|_error| "queued publication failed")
            }),
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND query LIKE '%pg_advisory_lock_shared%')",
                ).bind(pid).fetch_one(observer).await.unwrap();
                if waiting { break; }
                assert!(!pending.worker.is_finished(), "publication bypassed exclusive GC owner");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        pending.assert_blocked(observer).await;
        pending
    }

    pub(super) async fn assert_blocked(&self, observer: &sqlx::PgPool) {
        assert!(!self.worker.is_finished());
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM shardline_file_records WHERE file_id=$1")
                .bind(FILE)
                .fetch_one(observer)
                .await
                .unwrap();
        assert_eq!(count, 0, "blocked publication must not create metadata");
    }

    pub(super) async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(20), &mut self.worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            self.backend.download_file(FILE, None, None).await.unwrap(),
            PAYLOAD
        );
    }
}
