//! Stop database production after a proof, retaining one traversal snapshot.

use super::{candidate_batch, shared_repository_scope_key};
use crate::{PostgresMetadataStoreError, PostgresRecordLocator, PostgresRecordStore, StoredRecord};
use shardline_protocol::RepositoryScope;

const FIRST_PAGE: i64 = 16;
const MAX_PAGE: i64 = 1_024;

#[cfg(test)]
mod tests;

pub(super) async fn visit<Visitor, Error>(
    store: &PostgresRecordStore,
    repository: &RepositoryScope,
    hash: &str,
    mut visitor: Visitor,
) -> Result<(), Error>
where
    PostgresMetadataStoreError: Into<Error>,
    Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), Error> + Send,
    Error: Send,
{
    let mut transaction = store.pool.begin().await.map_err(database_error::<Error>)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(database_error::<Error>)?;
    let scope = shared_repository_scope_key(Some(repository));
    let mut after: Option<String> = None;
    let mut page_size = FIRST_PAGE;
    loop {
        let mut count = 0_i64;
        {
            let mut rows = sqlx::query(
                "SELECT r.record_key, r.record_kind, r.scope_key, r.file_id,
                        r.content_hash, r.record,
                        FLOOR(EXTRACT(EPOCH FROM r.updated_at))::BIGINT AS modified_since_epoch
                 FROM shardline_record_membership m
                 JOIN shardline_file_records r ON r.record_key=m.record_key
                 WHERE m.scope_key=$1 AND m.member_hash=$2 AND r.scope_key=$1
                   AND ($3::TEXT IS NULL OR m.record_key>$3)
                   AND ($3::TEXT IS NULL OR r.record_key>$3)
                 ORDER BY m.record_key LIMIT $4",
            )
            .bind(&scope)
            .bind(hash)
            .bind(after.clone())
            .bind(page_size)
            // Specialize both hash frequency and the optional seek boundary.
            // Bound both join inputs: a merge join otherwise rescans earlier
            // file records on every later membership page.
            .persistent(false)
            .fetch(&mut *transaction);
            loop {
                // Preserve the existing global conversion admission and the
                // 16-row/64-KiB-plus-one-row owned conversion bound. SQL page
                // growth never becomes a fetch_all or an owned page buffer.
                let batch = candidate_batch::read(&mut rows)
                    .await
                    .map_err(|error| -> Error { error.into() })?;
                for record in batch.records {
                    let record = record.map_err(|error| -> Error { error.into() })?;
                    after = Some(record.locator.record_key().to_owned());
                    count = count.saturating_add(1);
                    visitor(record)?;
                }
                if let Some(error) = batch.error {
                    return Err(error.into());
                }
                if batch.finished {
                    break;
                }
            }
        }
        if count < page_size {
            break;
        }
        // Larger traversals amortize round trips without ever asking the
        // database to produce an unbounded unused tail. Dropping this future
        // or returning a visitor error drops the read-only transaction.
        page_size = page_size.saturating_mul(4).min(MAX_PAGE);
    }
    transaction.commit().await.map_err(database_error::<Error>)
}

fn database_error<Error>(error: sqlx::Error) -> Error
where
    PostgresMetadataStoreError: Into<Error>,
{
    PostgresMetadataStoreError::from(error).into()
}
