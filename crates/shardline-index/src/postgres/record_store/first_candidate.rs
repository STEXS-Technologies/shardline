//! One bounded statement avoids draining a shared hash's entire result set.

use super::{
    admit_record_conversion, run_record_conversion, shared_repository_scope_key,
    stored_record_from_row,
};
use crate::{
    PostgresMetadataStoreError, PostgresRecordLocator, PostgresRecordStore, RecordStoreFuture,
    StoredRecord,
};
use shardline_protocol::RepositoryScope;

pub(super) fn read<'operation>(
    store: &'operation PostgresRecordStore,
    repository: &'operation RepositoryScope,
    hash: &'operation str,
) -> RecordStoreFuture<
    'operation,
    Option<StoredRecord<PostgresRecordLocator>>,
    PostgresMetadataStoreError,
> {
    Box::pin(async move {
        let permit = admit_record_conversion().await?;
        let row = sqlx::query(
            "SELECT r.record_key, r.record_kind, r.scope_key, r.file_id,
                    r.content_hash, r.record,
                    FLOOR(EXTRACT(EPOCH FROM r.updated_at))::BIGINT AS modified_since_epoch
             FROM shardline_record_membership m
             JOIN shardline_file_records r ON r.record_key=m.record_key
             WHERE m.scope_key=$1 AND m.member_hash=$2 AND r.scope_key=$1
             ORDER BY m.record_key LIMIT 1",
        )
        .bind(shared_repository_scope_key(Some(repository)))
        .bind(hash)
        // LIMIT 1 is sensitive to hash frequency: a generic plan trained on
        // shared content can scan the record-key index for an absent hash.
        // Keep parameter-aware planning local to this probe, without changing
        // connection-wide planner settings or the authoritative join.
        .persistent(false)
        .fetch_optional(&store.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        run_record_conversion(permit, move || stored_record_from_row(&row))
            .await
            .map(Some)
    })
}
