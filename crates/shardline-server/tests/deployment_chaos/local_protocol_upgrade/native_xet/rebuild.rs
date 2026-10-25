//! Recover derived descriptors after actual predecessor writes and rollback.
use super::*;

pub(super) async fn descriptors(root: &Path, postgres: Option<&str>, clear: bool) -> i64 {
    if let Some(url) = postgres {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(url)
            .await
            .unwrap();
        sqlx::query("SET statement_timeout='10s'")
            .execute(&pool)
            .await
            .unwrap();
        if clear {
            sqlx::query("DELETE FROM shardline_xet_xorbs")
                .execute(&pool)
                .await
                .unwrap();
        }
        let count = sqlx::query_scalar("SELECT COUNT(*) FROM shardline_xet_xorbs")
            .fetch_one(&pool)
            .await
            .unwrap();
        pool.close().await;
        count
    } else {
        let connection = rusqlite::Connection::open(root.join("metadata.sqlite3")).unwrap();
        if clear {
            connection
                .execute("DELETE FROM shardline_xet_xorbs", [])
                .unwrap();
        }
        connection
            .query_row("SELECT COUNT(*) FROM shardline_xet_xorbs", [], |row| {
                row.get(0)
            })
            .unwrap()
    }
}
