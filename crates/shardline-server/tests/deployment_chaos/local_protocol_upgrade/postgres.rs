//! Actual released/new server processes share an isolated PostgreSQL schema.
use super::*;

#[tokio::test]
async fn released_postgres_rolling_upgrade_preserves_live_writes_and_old_restart() {
    // The Docker-qualified maintenance gate owns all its fixtures. A missing
    // optional external URL must not silently skip its rolling-upgrade case.
    #[cfg(feature = "docker")]
    let services = if std::env::var_os("DATABASE_URL").is_none() {
        Some(
            shardline_test_support::DockerLocalStack::builder()
                .with_postgres()
                .start()
                .expect("PostgreSQL rolling upgrade fixture")
                .expect("PostgreSQL rolling upgrade requires Docker"),
        )
    } else {
        None
    };
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) => {
            #[cfg(feature = "docker")]
            {
                services
                    .as_ref()
                    .and_then(|stack| stack.postgres_url())
                    .expect("PostgreSQL fixture URL")
            }
            #[cfg(not(feature = "docker"))]
            {
                assert!(
                    !shardline_test_support::native_clients_required(),
                    "required PostgreSQL rolling upgrade needs DATABASE_URL or Docker feature"
                );
                // Also honor database-only qualification, even without the
                // native-client gate. Optional developer runs may still skip.
                let _ = shardline_test_support::postgres_fixture_url("DATABASE_URL");
                eprintln!(
                    "SKIPPED: PostgreSQL rolling upgrade requires DATABASE_URL or Docker feature"
                );
                return;
            }
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("invalid PostgreSQL rolling upgrade configuration")
        }
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
