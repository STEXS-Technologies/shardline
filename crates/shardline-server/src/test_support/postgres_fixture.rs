#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Disposable PostgreSQL schema and payload root based on the released ledger.
use crate::{ServerBackend, ServerConfig};
use shardline_index::PostgresIndexStore;
use sqlx::PgPool;
use std::num::NonZeroUsize;

pub(crate) struct PostgresFixture {
    _root: tempfile::TempDir,
    pub(crate) config: ServerConfig,
    pub(crate) backend: ServerBackend,
    pub(crate) admin: PgPool,
    pub(crate) schema: String,
}

impl PostgresFixture {
    pub(crate) async fn new() -> Option<Self> {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            return None;
        };
        let admin = PgPool::connect(&database_url).await.unwrap();
        let mut nonce = [0_u8; 8];
        getrandom::fill(&mut nonce).unwrap();
        let schema = format!("task_store_{}", hex::encode(nonce));
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'
             AND tablename LIKE 'shardline_%' AND tablename<>'shardline_task_artifacts_v1'
             ORDER BY tablename",
        )
        .fetch_all(&admin)
        .await
        .unwrap();
        for table in tables {
            let quoted = format!("\"{}\"", table.replace('"', "\"\""));
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {schema}.{quoted} (LIKE public.{quoted} INCLUDING ALL)"
            )))
            .execute(&admin)
            .await
            .unwrap();
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.shardline_schema_migrations SELECT * FROM public.shardline_schema_migrations"
        ))).execute(&admin).await.unwrap();
        let mut url = url::Url::parse(&database_url).unwrap();
        let options = url
            .query_pairs()
            .find(|(key, _)| key == "options")
            .map_or_else(String::new, |(_, value)| value.into_owned());
        let parameters: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "options")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(parameters)
            .append_pair("options", &format!("{options} -csearch_path={schema}"));
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_index_postgres_url(url.to_string())
        .unwrap();
        let backend = crate::PostgresBackend::new(
            root.path().to_path_buf(),
            config.public_base_url().to_owned(),
            NonZeroUsize::new(4096).unwrap(),
            url.as_str(),
        )
        .await
        .unwrap()
        .with_single_connection_pools(url.as_str())
        .unwrap();
        Some(Self {
            _root: root,
            config,
            backend: ServerBackend::Postgres(backend),
            admin,
            schema,
        })
    }

    pub(crate) fn postgres(&self) -> &crate::PostgresBackend {
        let backend = if let ServerBackend::Postgres(backend) = &self.backend {
            Some(backend)
        } else {
            None
        };
        backend.expect("fixture constructs PostgreSQL")
    }

    pub(crate) fn index(&self) -> &PostgresIndexStore {
        self.postgres().index_store()
    }

    pub(crate) async fn finish(self) {
        self.index().pool().close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}
