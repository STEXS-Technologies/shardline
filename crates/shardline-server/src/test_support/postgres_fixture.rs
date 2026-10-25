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
        let database_url = shardline_test_support::postgres_fixture_url("DATABASE_URL")?;
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
             AND tablename NOT IN ('shardline_hub_creation_policies','shardline_hub_creation_denials','shardline_hub_creation_events')
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
        // The owner migration installs the policy guard and its tables together.
        // Do not clone its ALTER TABLE additions without installation history.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {schema}.shardline_hub_repos
                 DROP COLUMN IF EXISTS creation_authorized,
                 DROP COLUMN IF EXISTS creation_issuer,
                 DROP COLUMN IF EXISTS creation_subject"
        )))
        .execute(&admin)
        .await
        .unwrap();
        // LIKE INCLUDING ALL does not copy foreign keys. Preserve the new
        // descriptor/member relation inside the disposable schema as well.
        let has_xet_chunks: bool = sqlx::query_scalar(
            "SELECT to_regclass(format('%I.shardline_xet_chunks',$1)) IS NOT NULL",
        )
        .bind(&schema)
        .fetch_one(&admin)
        .await
        .unwrap();
        if has_xet_chunks {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {schema}.shardline_xet_chunks ADD FOREIGN KEY (xorb_hash)
                 REFERENCES {schema}.shardline_xet_xorbs(xorb_hash) ON DELETE CASCADE"
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
        // Reinstall the schema-bound guard rather than copying triggers whose
        // functions resolve tables in public. Retain its real owner history;
        // older additive fixtures still exercise their existing adoption path.
        sqlx::raw_sql(include_str!(
            "../../migrations/20261024000000_hub_creation_policy.up.sql"
        ))
        .execute(backend.index_store().pool())
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.shardline_frontend_schema_v1
             SELECT * FROM public.shardline_frontend_schema_v1 WHERE version='20261024000000'"
        )))
        .execute(&admin)
        .await
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

    /// Complete owner-installed additive state before exercising router startup.
    /// Raw migration tests deliberately keep `new()`'s cloned history untouched.
    pub(crate) async fn migrate(&self) {
        crate::database_migration::run_database_migration(
            &crate::database_migration::DatabaseMigrationOptions::new(
                self.config.index_postgres_url().unwrap().to_owned(),
                crate::database_migration::DatabaseMigrationCommand::Up { steps: None },
            ),
        )
        .await
        .unwrap();
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
