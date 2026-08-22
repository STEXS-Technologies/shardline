use std::str::FromStr;
use std::time::Duration;

use futures_util::TryStreamExt;
use serde_json::to_vec;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow, query, query_scalar, types::Json};

use super::{PostgresMetadataStoreError, PostgresRecordLocator, RecordKind, i64_to_u64};
use crate::{
    DedupeShardMapping, FileRecord, RecordMutation, RecordStoreFuture, RecordTraversal,
    RepositoryRecordScope, StoredRecord,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_record_scope_key as shared_repository_record_scope_key,
        repository_scope_key as shared_repository_scope_key,
    },
    xet_hash_hex_string,
};

impl super::PostgresRecordStore {
    /// Inserts or replaces an immutable version record.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the row cannot be written.
    pub async fn insert_version_record(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let locator = self.version_record_locator(record);
        self.upsert_record(&locator, record).await
    }

    /// Atomically commits one file-version record and its latest-file alias.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when either row cannot be written. No
    /// latest-file alias is made visible unless the immutable version record is also
    /// committed.
    pub async fn commit_file_version_metadata(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let version_locator = self.version_record_locator(record);
        upsert_record_in_transaction(&mut transaction, &version_locator, record).await?;
        let latest_locator = self.latest_record_locator(record);
        upsert_record_in_transaction(&mut transaction, &latest_locator, record).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically removes a visible file reference and its immutable version record.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when the transaction cannot be completed.
    pub async fn delete_file_version_metadata(
        &self,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        let latest = self.latest_record_locator(record);
        let version = self.version_record_locator(record);
        query("DELETE FROM shardline_file_records WHERE record_key = $1 OR record_key = $2")
            .bind(&latest.record_key)
            .bind(&version.record_key)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically commits native shard metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresMetadataStoreError`] when any row in the metadata set cannot
    /// be written. No visible latest-file record or dedupe-shard mapping is committed
    /// unless the full set commits.
    pub async fn commit_native_shard_metadata(
        &self,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
    ) -> Result<(), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        for record in records {
            let locator = self.version_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        for mapping in dedupe_mappings {
            upsert_dedupe_shard_mapping_in_transaction(&mut transaction, mapping).await?;
        }
        for record in records {
            let locator = self.latest_record_locator(record);
            upsert_record_in_transaction(&mut transaction, &locator, record).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn upsert_record(
        &self,
        locator: &PostgresRecordLocator,
        record: &FileRecord,
    ) -> Result<(), PostgresMetadataStoreError> {
        query(
            "INSERT INTO shardline_file_records (
                record_key,
                record_kind,
                scope_key,
                file_id,
                content_hash,
                record
             )
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (record_key)
             DO UPDATE SET
                record_kind = EXCLUDED.record_kind,
                scope_key = EXCLUDED.scope_key,
                file_id = EXCLUDED.file_id,
                content_hash = EXCLUDED.content_hash,
                record = EXCLUDED.record,
                updated_at = now()",
        )
        .bind(&locator.record_key)
        .bind(locator.kind.as_str())
        .bind(&locator.scope_key)
        .bind(&locator.file_id)
        .bind(&record.content_hash)
        .bind(Json(record.clone()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn read_record(
        &self,
        locator: &PostgresRecordLocator,
    ) -> Result<Option<FileRecord>, PostgresMetadataStoreError> {
        let row = query("SELECT record FROM shardline_file_records WHERE record_key = $1")
            .bind(&locator.record_key)
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Json(record) = row.try_get::<Json<FileRecord>, _>("record")?;
        Ok(Some(record))
    }

    async fn list_record_locators(
        &self,
        kind: RecordKind,
    ) -> Result<Vec<PostgresRecordLocator>, PostgresMetadataStoreError> {
        let rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(record_locator_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn visit_record_locators<Visitor, VisitorError>(
        &self,
        kind: RecordKind,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(PostgresRecordLocator) -> Result<(), VisitorError>,
    {
        let mut rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(record_locator_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }

    async fn visit_records<Visitor, VisitorError>(
        &self,
        kind: RecordKind,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), VisitorError>,
    {
        let mut rows = query(
            "SELECT record_key,
                    record_kind,
                    scope_key,
                    file_id,
                    content_hash,
                    record,
                    FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT AS modified_since_epoch
             FROM shardline_file_records
             WHERE record_kind = $1
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(stored_record_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }

    async fn list_repository_record_locators(
        &self,
        kind: RecordKind,
        repository: &RepositoryRecordScope,
    ) -> Result<Vec<PostgresRecordLocator>, PostgresMetadataStoreError> {
        let scope_key = shared_repository_record_scope_key(repository);
        let escaped_prefix = format!(
            "{}%",
            scope_key
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%")
        );
        let rows = query(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = $1
               AND (scope_key = $2 OR scope_key LIKE $3 ESCAPE '\\')
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&escaped_prefix)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(record_locator_from_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn visit_repository_records<Visitor, VisitorError>(
        &self,
        kind: RecordKind,
        repository: &RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        PostgresMetadataStoreError: Into<VisitorError>,
        Visitor: FnMut(StoredRecord<PostgresRecordLocator>) -> Result<(), VisitorError>,
    {
        let scope_key = shared_repository_record_scope_key(repository);
        let escaped_prefix = format!(
            "{}%",
            scope_key
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%")
        );
        let mut rows = query(
            "SELECT record_key,
                    record_kind,
                    scope_key,
                    file_id,
                    content_hash,
                    record,
                    FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT AS modified_since_epoch
             FROM shardline_file_records
             WHERE record_kind = $1
               AND (scope_key = $2 OR scope_key LIKE $3 ESCAPE '\\')
             ORDER BY record_key",
        )
        .bind(kind.as_str())
        .bind(&scope_key)
        .bind(&escaped_prefix)
        .fetch(&self.pool);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(PostgresMetadataStoreError::from)
            .map_err(Into::into)?
        {
            visitor(stored_record_from_row(&row).map_err(Into::into)?)?;
        }

        Ok(())
    }
}

impl RecordTraversal for super::PostgresRecordStore {
    type Error = PostgresMetadataStoreError;
    type Locator = PostgresRecordLocator;
    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(RecordKind::Latest).await })
    }

    fn visit_latest_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_record_locators(RecordKind::Latest, &mut visitor)
                .await
        })
    }

    fn visit_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { self.visit_records(RecordKind::Latest, &mut visitor).await })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(RecordKind::Version).await })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(RecordKind::Latest, repository)
                .await
        })
    }

    fn visit_repository_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_repository_records(RecordKind::Latest, repository, &mut visitor)
                .await
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(RecordKind::Version, repository)
                .await
        })
    }

    fn visit_version_record_locators<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(Self::Locator) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_record_locators(RecordKind::Version, &mut visitor)
                .await
        })
    }

    fn visit_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { self.visit_records(RecordKind::Version, &mut visitor).await })
    }

    fn visit_repository_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        mut visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(StoredRecord<Self::Locator>) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move {
            self.visit_repository_records(RecordKind::Version, repository, &mut visitor)
                .await
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            let record = self
                .read_record(locator)
                .await?
                .ok_or(PostgresMetadataStoreError::RecordNotFound)?;
            Ok(to_vec(&record)?)
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            self.read_record(&locator)
                .await?
                .map(|stored_record| {
                    to_vec(&stored_record).map_err(PostgresMetadataStoreError::from)
                })
                .transpose()
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let exists = query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1 FROM shardline_file_records WHERE record_key = $1
                 )",
            )
            .bind(&locator.record_key)
            .fetch_one(&self.pool)
            .await?;
            Ok(exists)
        })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        Box::pin(async move {
            let value = query_scalar::<_, i64>(
                "SELECT FLOOR(EXTRACT(EPOCH FROM updated_at))::BIGINT
                 FROM shardline_file_records
                 WHERE record_key = $1",
            )
            .bind(&locator.record_key)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(PostgresMetadataStoreError::RecordNotFound)?;
            Ok(Duration::from_secs(i64_to_u64(value)?))
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        record_locator(RecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        record_locator(
            RecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
    }
}

impl RecordMutation for super::PostgresRecordStore {
    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let locator = self.version_record_locator(record);
            self.upsert_record(&locator, record).await
        })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            self.upsert_record(&locator, record).await
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let result = query("DELETE FROM shardline_file_records WHERE record_key = $1")
                .bind(&locator.record_key)
                .execute(&self.pool)
                .await?;
            if result.rows_affected() > 0 {
                return Ok(());
            }

            Err(PostgresMetadataStoreError::RecordNotFound)
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }
}

pub(super) async fn upsert_record_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    locator: &PostgresRecordLocator,
    record: &FileRecord,
) -> Result<(), PostgresMetadataStoreError> {
    query(
        "INSERT INTO shardline_file_records (
            record_key,
            record_kind,
            scope_key,
            file_id,
            content_hash,
            record
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (record_key)
         DO UPDATE SET
            record_kind = EXCLUDED.record_kind,
            scope_key = EXCLUDED.scope_key,
            file_id = EXCLUDED.file_id,
            content_hash = EXCLUDED.content_hash,
            record = EXCLUDED.record,
            updated_at = now()",
    )
    .bind(&locator.record_key)
    .bind(locator.kind.as_str())
    .bind(&locator.scope_key)
    .bind(&locator.file_id)
    .bind(&record.content_hash)
    .bind(Json(record.clone()))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn upsert_dedupe_shard_mapping_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    mapping: &DedupeShardMapping,
) -> Result<(), PostgresMetadataStoreError> {
    query(
        "INSERT INTO shardline_dedupe_shards (chunk_hash, shard_object_key)
         VALUES ($1, $2)
         ON CONFLICT (chunk_hash)
         DO UPDATE SET
            shard_object_key = EXCLUDED.shard_object_key,
            updated_at = now()",
    )
    .bind(xet_hash_hex_string(mapping.chunk_hash()))
    .bind(mapping.shard_object_key().as_str())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn record_locator_from_row(
    row: &PgRow,
) -> Result<PostgresRecordLocator, PostgresMetadataStoreError> {
    let kind = RecordKind::from_str(row.try_get::<String, _>("record_kind")?.as_str())
        .map_err(|_err| PostgresMetadataStoreError::InvalidRecordKind)?;
    let content_hash = match kind {
        RecordKind::Latest => None,
        RecordKind::Version => Some(row.try_get::<String, _>("content_hash")?),
    };
    Ok(PostgresRecordLocator {
        record_key: row.try_get("record_key")?,
        kind,
        scope_key: row.try_get("scope_key")?,
        file_id: row.try_get("file_id")?,
        content_hash,
    })
}

fn stored_record_from_row(
    row: &PgRow,
) -> Result<StoredRecord<PostgresRecordLocator>, PostgresMetadataStoreError> {
    let locator = record_locator_from_row(row)?;
    let Json(record) = row.try_get::<Json<FileRecord>, _>("record")?;
    let modified_since_epoch =
        Duration::from_secs(i64_to_u64(row.try_get::<i64, _>("modified_since_epoch")?)?);
    Ok(StoredRecord {
        locator,
        bytes: to_vec(&record)?,
        modified_since_epoch,
    })
}

pub(super) fn record_locator(
    kind: RecordKind,
    record: &FileRecord,
    content_hash: Option<String>,
) -> PostgresRecordLocator {
    let scope_key = shared_repository_scope_key(record.repository_scope.as_ref());
    let record_key = shared_record_key(
        kind.as_str(),
        &scope_key,
        &record.file_id,
        content_hash.as_deref(),
    );
    PostgresRecordLocator {
        record_key,
        kind,
        scope_key,
        file_id: record.file_id.clone(),
        content_hash,
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::RecordKind;
    use super::record_locator;
    use crate::{FileChunkRecord, FileRecord};

    fn sample_record(repo_scope: Option<RepositoryScope>) -> FileRecord {
        FileRecord {
            file_id: "test.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 100,
            chunk_size: 50,
            storage_repr: crate::StorageRepresentation::FixedChunkV1,
            repository_scope: repo_scope,
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 50,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 50,
            }],
        }
    }

    #[test]
    fn record_locator_produces_latest_locator_without_content_hash() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap();
        let record = sample_record(Some(scope));
        let locator = record_locator(RecordKind::Latest, &record, None);

        assert_eq!(locator.file_id(), "test.bin");
        assert!(locator.content_hash().is_none());
        // Latest locator key should not include content hash
        assert!(!locator.record_key().contains(&"a".repeat(64)));
    }

    #[test]
    fn record_locator_produces_version_locator_with_content_hash() {
        let record = sample_record(None);
        let locator = record_locator(
            RecordKind::Version,
            &record,
            Some(record.content_hash.clone()),
        );

        assert_eq!(locator.file_id(), "test.bin");
        assert_eq!(locator.content_hash(), Some(record.content_hash.as_str()));
        // Version locator key should include content hash
        assert!(locator.record_key().contains(&record.content_hash));
    }

    #[test]
    fn record_locator_latest_and_version_have_different_keys() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "team", "assets", None).unwrap();
        let record = sample_record(Some(scope));
        let latest = record_locator(RecordKind::Latest, &record, None);
        let version = record_locator(
            RecordKind::Version,
            &record,
            Some(record.content_hash.clone()),
        );

        assert_ne!(latest.record_key(), version.record_key());
        assert_eq!(latest.file_id(), version.file_id());
    }

    #[test]
    fn record_locator_uses_repository_scope_key() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
        let record = sample_record(Some(scope));
        let locator = record_locator(RecordKind::Latest, &record, None);

        // The locator key should contain scope information
        assert!(locator.record_key().contains("github"));
    }

    #[test]
    fn record_locator_without_repository_scope() {
        let record = sample_record(None);
        let locator = record_locator(RecordKind::Latest, &record, None);

        // Without scope, the key should still be valid
        assert!(!locator.record_key().is_empty());
        assert_eq!(locator.file_id(), "test.bin");
    }
}
