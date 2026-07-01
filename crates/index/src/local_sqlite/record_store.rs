use std::time::Duration;

use rusqlite::{OptionalExtension, params};
use shardline_protocol::unix_now_seconds_lossy;

use super::{
    LocalIndexStoreError, LocalRecordKind, LocalRecordLocator, LocalRecordStore, i64_to_u64,
    record_not_found_error,
};
use crate::{
    FileRecord, RecordMutation, RecordStoreFuture, RecordTraversal, RepositoryRecordScope,
};

impl RecordTraversal for LocalRecordStore {
    type Error = LocalIndexStoreError;
    type Locator = LocalRecordLocator;

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.list_record_locators(LocalRecordKind::Latest))
                .await
                .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        let repository = repository.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.list_repository_record_locators(LocalRecordKind::Latest, &repository)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.list_record_locators(LocalRecordKind::Version)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        let repository = repository.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.list_repository_record_locators(LocalRecordKind::Version, &repository)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        let store = self.clone();
        let locator = locator.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store
                    .read_record_bytes_raw(&locator)?
                    .ok_or_else(record_not_found_error)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        let store = self.clone();
        let record = record.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let locator = store.latest_record_locator(&record);
                store.read_record_bytes_raw(&locator)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        let store = self.clone();
        let locator = locator.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
                let exists = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM shardline_file_records WHERE record_key = ?1
                     )",
                    params![locator.record_key()],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok(exists != 0)
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        let store = self.clone();
        let locator = locator.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
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
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
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
        let store = self.clone();
        let record = record.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
                let locator = store.version_record_locator(&record);
                super::helpers::upsert_file_record_row(
                    &connection,
                    &locator,
                    &record,
                    unix_now_seconds_lossy(),
                )?;
                Ok(())
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let record = record.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
                let locator = store.latest_record_locator(&record);
                super::helpers::upsert_file_record_row(
                    &connection,
                    &locator,
                    &record,
                    unix_now_seconds_lossy(),
                )?;
                Ok(())
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        let store = self.clone();
        let locator = locator.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let connection = store.open_connection()?;
                let deleted = connection.execute(
                    "DELETE FROM shardline_file_records WHERE record_key = ?1",
                    params![locator.record_key()],
                )?;
                if deleted == 0 {
                    return Err(record_not_found_error());
                }
                Ok(())
            })
            .await
            .map_err(|_error| LocalIndexStoreError::BlockingTask)?
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }
}
