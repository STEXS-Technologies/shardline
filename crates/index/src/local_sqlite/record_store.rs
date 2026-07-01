use std::time::Duration;

use rusqlite::{OptionalExtension, params};
use shardline_protocol::unix_now_seconds_lossy;

use super::{
    LocalIndexStoreError, LocalRecordKind, LocalRecordLocator, LocalRecordStore,
    i64_to_u64, record_not_found_error,
};
use crate::{FileRecord, RecordMutation, RecordStore, RecordStoreFuture, RecordTraversal, RepositoryRecordScope};

use RecordTraversal as _;

impl RecordTraversal for LocalRecordStore {
    type Error = LocalIndexStoreError;
    type Locator = LocalRecordLocator;

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| self.list_record_locators(LocalRecordKind::Latest))
        })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                self.list_repository_record_locators(LocalRecordKind::Latest, repository)
            })
        })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| self.list_record_locators(LocalRecordKind::Version))
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                self.list_repository_record_locators(LocalRecordKind::Version, repository)
            })
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                self.read_record_bytes_raw(locator)?
                    .ok_or_else(record_not_found_error)
            })
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let locator = self.latest_record_locator(record);
                self.read_record_bytes_raw(&locator)
            })
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let connection = self.open_connection()?;
                let exists = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM shardline_file_records WHERE record_key = ?1
                     )",
                    params![locator.record_key()],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok(exists != 0)
            })
        })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let connection = self.open_connection()?;
                let value = connection
                    .query_row(
                        "SELECT updated_at_unix_seconds
                         FROM shardline_file_records
                         WHERE record_key = ?1",
                        params![locator.record_key()],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .ok_or_else(record_not_found_error)?;
                Ok(Duration::from_secs(i64_to_u64(value)?))
            })
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        super::helpers::local_record_locator(LocalRecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        super::helpers::local_record_locator(
            LocalRecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
    }
}

impl RecordMutation for LocalRecordStore {
    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let connection = self.open_connection()?;
                let locator = self.version_record_locator(record);
                super::helpers::upsert_file_record_row(
                    &connection,
                    &locator,
                    record,
                    unix_now_seconds_lossy(),
                )?;
                Ok(())
            })
        })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let connection = self.open_connection()?;
                let locator = self.latest_record_locator(record);
                super::helpers::upsert_file_record_row(
                    &connection,
                    &locator,
                    record,
                    unix_now_seconds_lossy(),
                )?;
                Ok(())
            })
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                let connection = self.open_connection()?;
                let deleted = connection.execute(
                    "DELETE FROM shardline_file_records WHERE record_key = ?1",
                    params![locator.record_key()],
                )?;
                if deleted == 0 {
                    return Err(record_not_found_error());
                }
                Ok(())
            })
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }
}
