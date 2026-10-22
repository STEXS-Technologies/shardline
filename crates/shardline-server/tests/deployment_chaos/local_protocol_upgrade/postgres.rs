//! Actual released/new server processes share an isolated PostgreSQL schema.
use super::*;

#[tokio::test]
async fn released_postgres_rolling_upgrade_preserves_live_writes_and_old_restart() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIPPED: PostgreSQL rolling upgrade requires DATABASE_URL");
        return;
    };
    assert!(resolve_n_minus_one_binary("PostgreSQL rolling upgrade").is_some());
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).unwrap();
    let schema = format!("protocol_upgrade_{}", hex::encode(nonce));
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await
        .unwrap();
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
    rolling_upgrade(Some(url.as_str())).await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
