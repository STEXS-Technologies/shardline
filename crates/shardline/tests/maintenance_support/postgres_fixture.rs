use super::super::*;

pub(super) async fn connect_owned() -> Result<(String, sqlx::PgPool)> {
    let url = std::env::var("SHARDLINE_OWNED_PG_FIXTURE_URL")?;
    let token = std::env::var("SHARDLINE_OWNED_PG_FIXTURE_TOKEN")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let saved: String = sqlx::query_scalar("SELECT token FROM shardline_validation_owner")
        .fetch_one(&pool)
        .await?;
    assert_eq!(saved, token, "test must run only in its owned fixture");
    Ok((url, pool))
}
