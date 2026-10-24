//! Bound publication round trips while retaining ordered duplicate-path writes.
use super::{HubFileEntry, PostgresMetadataStoreError, u64_to_i64};
use std::collections::HashSet;

const FILES_PER_STATEMENT: usize = 1000;

pub(super) async fn store(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    commit: &str,
    files: &[HubFileEntry],
) -> Result<(), PostgresMetadataStoreError> {
    let mut remaining = files.iter().peekable();
    while remaining.peek().is_some() {
        let mut paths = HashSet::new();
        let mut batch = Vec::new();
        while batch.len() < FILES_PER_STATEMENT {
            let Some(&file) = remaining.peek() else { break };
            // PostgreSQL cannot update a conflicting key twice in one INSERT.
            // Start a new statement instead of dropping intermediate writes.
            if !paths.insert(file.path.as_str()) {
                break;
            }
            batch.push((file, u64_to_i64(file.size)?));
            let _consumed = remaining.next();
        }
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO shardline_hub_file_entries (commit_sha, path, size, sha, is_lfs) ",
        );
        query.push_values(batch, |mut row, (file, size)| {
            row.push_bind(commit)
                .push_bind(&file.path)
                .push_bind(size)
                .push_bind(&file.sha)
                .push_bind(file.is_lfs);
        });
        query.push(
            " ON CONFLICT (commit_sha, path)
              DO UPDATE SET size = EXCLUDED.size, sha = EXCLUDED.sha, is_lfs = EXCLUDED.is_lfs
              WHERE shardline_hub_file_entries.size IS DISTINCT FROM EXCLUDED.size
                 OR shardline_hub_file_entries.sha IS DISTINCT FROM EXCLUDED.sha
                 OR shardline_hub_file_entries.is_lfs IS DISTINCT FROM EXCLUDED.is_lfs",
        );
        query.build().execute(&mut **tx).await?;
    }
    Ok(())
}
