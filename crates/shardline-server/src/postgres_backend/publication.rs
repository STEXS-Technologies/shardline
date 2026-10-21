//! Final publication of newly staged chunks on the connection owning the write.

use futures_util::{TryStreamExt, stream};
use shardline_index::{
    FileRecord, PostgresMetadataStoreError, ResumableCompletionFence, S3ObjectEntry,
    S3PublishCondition,
};
use shardline_storage::AsyncObjectStore;
use sqlx::{Connection as _, PgConnection};

use crate::{ServerError, chunk_store::chunk_object_key, object_store::ServerObjectStore};

impl super::PostgresBackend {
    #[cfg(test)]
    pub(super) async fn commit_validated_native_shard(
        &self,
        records: &[FileRecord],
        mappings: &[shardline_index::DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
    ) -> Result<(), crate::xet_adapter::XetAdapterError> {
        self.commit_validated_native_shard_with_xet_identities(records, mappings, references, &[])
            .await
    }

    pub(super) async fn commit_validated_native_shard_with_xet_identities(
        &self,
        records: &[FileRecord],
        mappings: &[shardline_index::DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
        identities: &[shardline_index::XetFileIdentity],
    ) -> Result<(), crate::xet_adapter::XetAdapterError> {
        let mut connection = self
            .record_store
            .pool()
            .acquire()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        self.commit_validated_native_shard_on_connection_with_xet_identities(
            &mut connection,
            records,
            mappings,
            references,
            identities,
        )
        .await
    }

    /// Recheck validated native backing objects before the transaction releases
    /// reference locks, including the Xet identities published with the shard.
    pub(super) async fn commit_validated_native_shard_on_connection_with_xet_identities(
        &self,
        connection: &mut PgConnection,
        records: &[FileRecord],
        mappings: &[shardline_index::DedupeShardMapping],
        references: &[shardline_storage::ObjectKey],
        identities: &[shardline_index::XetFileIdentity],
    ) -> Result<(), crate::xet_adapter::XetAdapterError> {
        let mut transaction = connection
            .begin()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        self.record_store
            .commit_native_shard_metadata_on_connection_with_xet_identities(
                &mut transaction,
                records,
                mappings,
                references,
                identities,
            )
            .await?;
        let protected: bool = sqlx::query_scalar(
            "SELECT to_regprocedure(format('%I.shardline_reference_keys_remote_delete_v1(text,text[])', current_schema())) IS NOT NULL",
        ).fetch_one(&mut *transaction).await
            .map_err(PostgresMetadataStoreError::from)?;
        if protected && !matches!(self.object_store, ServerObjectStore::Blackhole) {
            let mut keys = records
                .iter()
                .flat_map(|record| &record.chunks)
                .map(|chunk| crate::xet_adapter::xorb_object_key(&chunk.hash))
                .collect::<Result<Vec<_>, _>>()?;
            for mapping in mappings {
                keys.push(mapping.shard_object_key().clone());
            }
            keys.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
            keys.dedup();
            stream::iter(
                keys.into_iter()
                    .map(Ok::<_, crate::xet_adapter::XetAdapterError>),
            )
            .try_for_each_concurrent(self.upload_max_in_flight_chunks.get(), |key| async move {
                if AsyncObjectStore::metadata(&self.object_store, &key)
                    .await?
                    .is_none()
                {
                    return Err(crate::error::publication::native_content_unavailable_io().into());
                }
                Ok(())
            })
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        Ok(())
    }

    /// Caller must keep its transaction open through metadata publication.
    /// This applies only to fresh ingestor records: a hash names either a
    /// packed xorb or a direct CAS chunk. Legacy records can instead depend on
    /// index mappings and must not be checked using this fresh-upload rule.
    async fn verify_staged_file_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
    ) -> Result<(), ServerError> {
        let function: Option<String> = sqlx::query_scalar(
            "SELECT CASE WHEN to_regprocedure(format('%I.shardline_reference_keys_remote_delete_v1(text,text[])', current_schema())) IS NOT NULL
             THEN format('%I.shardline_reference_keys_remote_delete_v1', current_schema()) END",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(PostgresMetadataStoreError::from)?;
        // Preserve pre-owner-setup deployments. Guarded deletes cannot start
        // until the existing owner migration installs the matching helpers.
        let Some(function) = function else {
            return Ok(());
        };
        let mut hashes: Vec<_> = record
            .chunks
            .iter()
            .map(|chunk| chunk.hash.as_str())
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        let pairs = hashes
            .into_iter()
            .map(|hash| {
                Ok((
                    chunk_object_key(hash)?,
                    crate::xet_adapter::xorb_object_key(hash)?,
                ))
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        let mut keys = Vec::new();
        // Include the same sidecar slots as the file-record SQL trigger so it
        // does not acquire additional lower-numbered slots after this check.
        for (chunk, xorb) in &pairs {
            keys.extend([chunk.clone(), xorb.clone()]);
            keys.push(
                shardline_storage::ObjectKey::parse(&format!("_xorb_chunks/{}", chunk.as_str(),))
                    .map_err(|_invalid_key| ServerError::InvalidContentHash)?,
            );
        }
        keys.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        keys.dedup();
        let physical: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
        // The identifier comes from PostgreSQL's identifier-quoting function.
        // The helper locks the same bounded slots as DELETE, rejects pending
        // attempts, and keeps those locks until this transaction commits.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {function}(current_schema(), $1)"
        )))
        .bind(physical)
        .execute(&mut *connection)
        .await
        .map_err(PostgresMetadataStoreError::from)?;
        if matches!(self.object_store, ServerObjectStore::Blackhole) {
            return Ok(());
        }
        stream::iter(pairs.into_iter().map(Ok::<_, ServerError>))
            .try_for_each_concurrent(
                self.upload_max_in_flight_chunks.get(),
                |(chunk, xorb)| async move {
                    if AsyncObjectStore::metadata(&self.object_store, &xorb)
                        .await?
                        .is_none()
                        && AsyncObjectStore::metadata(&self.object_store, &chunk)
                            .await?
                            .is_none()
                    {
                        return Err(crate::error::publication::content_unavailable());
                    }
                    Ok(())
                },
            )
            .await
    }

    pub(super) async fn commit_staged_file_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
    ) -> Result<(), ServerError> {
        let mut transaction = connection
            .begin()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        self.record_store
            .stage_file_version_metadata_in_transaction(&mut transaction, record)
            .await?;
        self.verify_staged_file_on_connection(&mut transaction, record)
            .await?;
        transaction
            .commit()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        Ok(())
    }

    pub(crate) async fn publish_prepared_s3_on_connection(
        &self,
        connection: &mut PgConnection,
        record: &FileRecord,
        entry: &S3ObjectEntry,
        condition: &S3PublishCondition,
        completion_fence: Option<&ResumableCompletionFence>,
    ) -> Result<bool, ServerError> {
        let mut transaction = connection
            .begin()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        let published = self
            .record_store
            .publish_s3_object_on_connection(
                &mut transaction,
                record,
                entry,
                condition,
                completion_fence,
            )
            .await?;
        if published {
            self.verify_staged_file_on_connection(&mut transaction, record)
                .await?;
        }
        transaction
            .commit()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        Ok(published)
    }
}

#[cfg(test)]
mod tests;
