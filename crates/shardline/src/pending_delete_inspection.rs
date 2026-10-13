//! Read-only pending-delete inspection without a server backend or storage requests.
use std::path::Path;

pub(crate) async fn run(
    root: Option<&Path>,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let epoch = after
        .as_deref()
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_error| "pending-delete --after must be an attempt number from next_after")?
        .unwrap_or(0);
    if epoch > i64::MAX.unsigned_abs() || !(1..=100).contains(&limit) {
        return Err("invalid pending-delete page bounds".into());
    }
    let config = crate::config::load_server_config(root, None)?;
    let pending = if let Some(url) = config.index_postgres_url() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect_lazy(url)?;
        let result = shardline_index::PostgresIndexStore::new(pool.clone())
            .pending_remote_deletes(epoch, limit.saturating_add(1))
            .await;
        pool.close().await;
        result?
    } else {
        None
    };
    Ok(serde_json::to_value(
        shardline_server::pending_delete_inspection_page(pending, limit)?,
    )?)
}
