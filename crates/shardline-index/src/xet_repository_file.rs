//! Candidate lookup across storage revisions within one verified repository.
//!
//! These results do not authorize a download. Callers must verify membership
//! in the requested immutable Hub commit before using the original scope.

use rusqlite::OptionalExtension;
use shardline_protocol::RepositoryScope;

use crate::{
    FileRecord, LocalIndexStoreError, LocalRecordStore, PostgresMetadataStoreError,
    PostgresRecordStore, XetFileIdentity,
};

fn valid(record: &FileRecord, scope: &RepositoryScope, file_id: &str, size: u64) -> bool {
    record.file_id == file_id
        && record.total_bytes == size
        && record.repository_scope.as_ref().is_some_and(|stored| {
            XetFileIdentity::repository_key_for_scope(stored)
                == XetFileIdentity::repository_key_for_scope(scope)
        })
}

impl LocalRecordStore {
    /// Finds a native file candidate and its Hub SHA-256 in this repository.
    /// This is a storage lookup, not a revision authorization decision.
    /// # Errors
    /// Returns an error on invalid metadata or a database failure.
    pub fn repository_xet_file(
        &self,
        scope: &RepositoryScope,
        file_id: &str,
    ) -> Result<Option<(String, FileRecord)>, LocalIndexStoreError> {
        let repository_key = XetFileIdentity::repository_key_for_scope(scope);
        let Some(upper) = crate::hub::prefix_successor(&repository_key) else {
            return Ok(None);
        };
        let connection = self.open_connection()?;
        let max_bytes =
            crate::local_sqlite::u64_to_i64(crate::local_sqlite::MAX_LOCAL_RECORD_METADATA_BYTES)?;
        let row: Option<(String, i64, String)> = connection
            .query_row(
                "SELECT i.sha256,i.total_bytes,r.record FROM shardline_xet_file_aliases i
             JOIN shardline_file_records r ON r.file_id=i.file_id
             WHERE i.scope_key=?1 AND i.file_id=?2 AND r.scope_key>=?1 AND r.scope_key<?3
               AND r.record_kind='latest' AND length(CAST(r.record AS BLOB))<=?4
             ORDER BY r.scope_key LIMIT 1",
                rusqlite::params![repository_key, file_id, upper, max_bytes],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((sha, size, bytes)) = row else {
            return Ok(None);
        };
        let record: FileRecord = serde_json::from_str(&bytes)?;
        let size = crate::local_sqlite::i64_to_u64(size)?;
        Ok(valid(&record, scope, file_id, size).then_some((sha, record)))
    }
}

impl PostgresRecordStore {
    /// Finds a native file candidate and its Hub SHA-256 in this repository.
    /// This is a storage lookup, not a revision authorization decision.
    /// # Errors
    /// Returns an error on invalid metadata or a database failure.
    pub async fn repository_xet_file(
        &self,
        scope: &RepositoryScope,
        file_id: &str,
    ) -> Result<Option<(String, FileRecord)>, PostgresMetadataStoreError> {
        let repository_key = XetFileIdentity::repository_key_for_scope(scope);
        let Some(upper) = crate::hub::prefix_successor(&repository_key) else {
            return Ok(None);
        };
        let permit = crate::postgres::admit_record_conversion().await?;
        let row: Option<(String, i64, String)> = sqlx::query_as(
            "SELECT i.sha256,i.total_bytes,r.record::text FROM shardline_xet_file_aliases i
             JOIN shardline_file_records r ON r.file_id=i.file_id
             WHERE i.scope_key=$1 AND i.file_id=$2 AND r.scope_key>=$1 AND r.scope_key<$3
               AND r.record_kind='latest' AND octet_length(r.record::text)<=$4
             ORDER BY r.scope_key LIMIT 1",
        )
        .bind(repository_key)
        .bind(file_id)
        .bind(upper)
        .bind(
            i64::try_from(crate::local_sqlite::MAX_LOCAL_RECORD_METADATA_BYTES).map_err(
                |error| PostgresMetadataStoreError::IntegerOutOfRange(error.to_string()),
            )?,
        )
        .fetch_optional(self.pool())
        .await?;
        let Some((sha, size, value)) = row else {
            return Ok(None);
        };
        let scope = scope.clone();
        let file_id = file_id.to_owned();
        crate::postgres::run_record_conversion(permit, move || {
            let record: FileRecord = serde_json::from_str(&value)?;
            let size = u64::try_from(size).map_err(|_error| {
                PostgresMetadataStoreError::IntegerOutOfRange(
                    "negative native file length".to_owned(),
                )
            })?;
            Ok(valid(&record, &scope, &file_id, size).then_some((sha, record)))
        })
        .await
    }
}
