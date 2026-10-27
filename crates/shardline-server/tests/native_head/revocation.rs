//! Exercise HTTP access against committed and rolled-back membership changes.
use super::*;

pub(super) struct Read<'fixture> {
    pub client: &'fixture Client,
    pub url: &'fixture str,
    pub credential: &'fixture str,
    pub proxy: &'fixture S3FaultProxy,
    pub bytes: &'fixture [u8],
}

enum Metadata {
    Local(rusqlite::Connection),
    Postgres(sqlx::PgPool),
}

impl Metadata {
    async fn counts(&self) -> (i64, i64) {
        let sql = "SELECT (SELECT count(*) FROM shardline_file_records),
                         (SELECT count(*) FROM shardline_record_membership)";
        match self {
            Self::Local(connection) => connection
                .query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap(),
            Self::Postgres(pool) => sqlx::query_as(sql).fetch_one(pool).await.unwrap(),
        }
    }

    async fn delete(&self, kind: &str) -> u64 {
        match self {
            Self::Local(connection) => u64::try_from(
                connection
                    .execute(
                        "DELETE FROM shardline_file_records WHERE record_kind=?1",
                        [kind],
                    )
                    .unwrap(),
            )
            .unwrap(),
            Self::Postgres(pool) => {
                sqlx::query("DELETE FROM shardline_file_records WHERE record_kind=$1")
                    .bind(kind)
                    .execute(pool)
                    .await
                    .unwrap()
                    .rows_affected()
            }
        }
    }
}

impl Read<'_> {
    pub(super) async fn allowed(&self) {
        let started = std::time::Instant::now();
        let before_heads = self.proxy.head_requests();
        let before_gets = self.proxy.get_requests();
        let response = self
            .client
            .get(self.url.replace("/v1/xorbs/", "/transfer/xorb/"))
            .header(
                reqwest::header::RANGE,
                format!("bytes=0-{}", self.bytes.len().checked_sub(1).unwrap()),
            )
            .bearer_auth(self.credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes().await.unwrap().as_ref(), self.bytes);
        let get_elapsed = started.elapsed();
        let head_started = std::time::Instant::now();
        assert_eq!(
            self.client
                .head(self.url)
                .bearer_auth(self.credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        eprintln!(
            "membership HTTP GET+HEAD: {} us (GET={} us HEAD={} us provider GET={} HEAD={})",
            started.elapsed().as_micros(),
            get_elapsed.as_micros(),
            head_started.elapsed().as_micros(),
            self.proxy.get_requests().checked_sub(before_gets).unwrap(),
            self.proxy
                .head_requests()
                .checked_sub(before_heads)
                .unwrap(),
        );
    }

    pub(super) async fn qualify(
        &self,
        root: &std::path::Path,
        database: Option<&str>,
        million: bool,
    ) {
        let metadata = match database {
            Some(database) => Metadata::Postgres(sqlx::PgPool::connect(database).await.unwrap()),
            None => {
                Metadata::Local(rusqlite::Connection::open(root.join("metadata.sqlite3")).unwrap())
            }
        };
        let expected = if million { 1_000_002 } else { 2 };
        assert_eq!(metadata.counts().await, (expected, expected));
        self.allowed().await;
        if million {
            let pool = match &metadata {
                Metadata::Postgres(pool) => Some(pool),
                Metadata::Local(_) => None,
            }
            .expect("million-record HTTP fixture requires PostgreSQL");
            // Turn the million shared-source candidates into unrelated records
            // using the real refresh trigger. The two genuine publications
            // remain authoritative; candidate density must not govern access.
            let changed = tokio::time::timeout(
                Duration::from_secs(600),
                sqlx::query(
                    "UPDATE shardline_file_records SET record =
                    jsonb_set(record,'{chunks,0,hash}',to_jsonb(repeat('e',64)))
                    WHERE file_id LIKE 'member-%'",
                )
                .execute(pool),
            )
            .await
            .unwrap()
            .unwrap()
            .rows_affected();
            assert_eq!(changed, 1_000_000);
            sqlx::raw_sql("ANALYZE shardline_file_records; ANALYZE shardline_record_membership")
                .execute(pool)
                .await
                .unwrap();
            let source_hash = self.url.rsplit('/').next().unwrap();
            let candidates: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM shardline_record_membership WHERE member_hash=$1",
            )
            .bind(source_hash)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(
                candidates, 2,
                "only the real latest/history publications authorize this source"
            );
            assert_eq!(metadata.counts().await, (expected, expected));
            eprintln!("sparse source among one million unrelated records:");
            self.allowed().await;
            super::denied_reads(
                self.client,
                self.url,
                &token("other", TokenScope::Read),
                self.proxy,
            )
            .await;
            let missing = format!(
                "{}/{}",
                self.url.rsplit_once('/').unwrap().0,
                "f".repeat(64)
            );
            super::denied_reads(self.client, &missing, self.credential, self.proxy).await;
            // Compare the same HTTP proof after reclaiming index entries left
            // by the bulk revocation. SQL-only plans cannot attribute the
            // complete response time or separate GET from HEAD/provider work.
            // Both pre-cleanup authorization and denial checks ran above.
            // Keep this diagnostic within the disposable container's shared
            // memory budget; parallel VACUUM can exceed Docker's default /dev/shm.
            for statement in [
                "VACUUM (ANALYZE, PARALLEL 0) shardline_file_records",
                "VACUUM (ANALYZE, PARALLEL 0) shardline_record_membership",
            ] {
                sqlx::query(statement).execute(pool).await.unwrap();
            }
            assert_eq!(metadata.counts().await, (expected, expected));
            eprintln!("sparse source after vacuum (same authoritative records):");
            for _ in 0..3 {
                self.allowed().await;
            }
            super::denied_reads(
                self.client,
                self.url,
                &token("other", TokenScope::Read),
                self.proxy,
            )
            .await;
        }
        assert_eq!(
            metadata.delete("latest").await,
            u64::try_from(expected.checked_sub(1).unwrap()).unwrap()
        );
        assert_eq!(metadata.counts().await, (1, 1));
        // Historical publications remain valid grants after latest disappears.
        self.allowed().await;
        match &metadata {
            Metadata::Local(connection) => {
                connection.execute_batch("BEGIN IMMEDIATE; DELETE FROM shardline_file_records WHERE record_kind='version';").unwrap();
                self.allowed().await;
                connection.execute_batch("ROLLBACK").unwrap();
            }
            Metadata::Postgres(pool) => {
                let mut transaction = pool.begin().await.unwrap();
                assert_eq!(
                    sqlx::query("DELETE FROM shardline_file_records WHERE record_kind='version'")
                        .execute(&mut *transaction)
                        .await
                        .unwrap()
                        .rows_affected(),
                    1
                );
                self.allowed().await;
                transaction.rollback().await.unwrap();
            }
        }
        assert_eq!(metadata.counts().await, (1, 1));
        assert_eq!(metadata.delete("version").await, 1);
        assert_eq!(metadata.counts().await, (0, 0));
        super::denied_reads(self.client, self.url, self.credential, self.proxy).await;
        if let Metadata::Postgres(pool) = metadata {
            pool.close().await;
        }
    }
}

pub(super) async fn populate(database: &str) {
    let pool = sqlx::PgPool::connect(database).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(600),
        sqlx::query(
            "WITH template AS (
            SELECT record,scope_key,content_hash FROM shardline_file_records
            WHERE record_kind='version' LIMIT 1), source AS (
            SELECT 'member-' || lpad(i::text,7,'0') AS file_id FROM generate_series(1,1000000) AS i)
         INSERT INTO shardline_file_records
           (record_key,record_kind,scope_key,file_id,content_hash,record)
         SELECT '6:latest' || length(scope_key)::text || ':' || scope_key ||
             length(file_id)::text || ':' || file_id,
           'latest',scope_key,file_id,content_hash,
           jsonb_set(record,'{file_id}',to_jsonb(file_id))
         FROM template CROSS JOIN source",
        )
        .execute(&pool),
    )
    .await
    .unwrap()
    .unwrap();
    sqlx::raw_sql("ANALYZE shardline_file_records; ANALYZE shardline_record_membership")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
