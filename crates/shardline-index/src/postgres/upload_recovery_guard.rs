//! Current writers identify themselves only within their atomic transition.
use sqlx::{Postgres, Transaction};

pub(super) async fn enter(
    transaction: &mut Transaction<'_, Postgres>,
    intent_id: &str,
    repository: &str,
) -> Result<Option<String>, sqlx::Error> {
    if !intent_id.starts_with("file-v2-")
        && !intent_id.starts_with("shard-v2-")
        && !repository.starts_with("scope-json-v1:")
    {
        return Ok(None);
    }
    let (previous, _marker): (String, String) = sqlx::query_as(
        "WITH previous AS MATERIALIZED (
             SELECT COALESCE(current_setting('shardline.upload_intent_writer_v2', true), '') AS value
         ) SELECT value, set_config('shardline.upload_intent_writer_v2', $1, true) FROM previous",
    )
    .bind(intent_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(Some(previous))
}

pub(super) async fn leave(
    transaction: &mut Transaction<'_, Postgres>,
    previous: Option<&str>,
) -> Result<(), sqlx::Error> {
    let Some(previous) = previous else {
        return Ok(());
    };
    // The caller may own an outer transaction. Restore before releasing our
    // savepoint rather than leaving a marker on its remaining work.
    sqlx::query("SELECT set_config('shardline.upload_intent_writer_v2', $1, true)")
        .bind(previous)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}
