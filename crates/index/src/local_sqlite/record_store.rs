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

#[cfg(test)]
mod tests {
    use shardline_protocol::{ChunkRange, ShardlineHash};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{FileChunkRecord, FileRecord, RecordMutation, RecordTraversal};

    fn make_store() -> LocalRecordStore {
        let storage = shardline_test_support::TempStorage::new();
        LocalRecordStore::new(storage.path_buf()).expect("failed to create local record store")
    }

    fn sample_record() -> FileRecord {
        FileRecord {
            file_id: "test.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_version_records_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let call_count = AtomicUsize::new(0);
        RecordTraversal::visit_version_records(&store, |_| {
            call_count.fetch_add(1, Ordering::SeqCst);
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_latest_records_on_empty_store_does_not_call_visitor() {
        let store = make_store();
        let call_count = AtomicUsize::new(0);
        RecordTraversal::visit_latest_records(&store, |_| {
            call_count.fetch_add(1, Ordering::SeqCst);
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_version_records_after_write_calls_visitor_with_correct_data() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_version_record(&store, &record)
            .await
            .expect("write should succeed");

        let mut visited = Vec::new();
        RecordTraversal::visit_version_records(&store, |stored| {
            visited.push(stored.bytes);
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(visited.len(), 1);
        let loaded: FileRecord = serde_json::from_slice(&visited[0]).expect("should deserialize");
        assert_eq!(loaded.file_id, record.file_id);
        assert_eq!(loaded.content_hash, record.content_hash);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_latest_records_after_write_calls_visitor_with_correct_data() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_latest_record(&store, &record)
            .await
            .expect("write should succeed");

        let mut visited = Vec::new();
        RecordTraversal::visit_latest_records(&store, |stored| {
            visited.push(stored.bytes);
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert_eq!(visited.len(), 1);
        let loaded: FileRecord = serde_json::from_slice(&visited[0]).expect("should deserialize");
        assert_eq!(loaded.file_id, record.file_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_and_read_version_record_roundtrip() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_version_record(&store, &record)
            .await
            .expect("write should succeed");

        let locator = RecordTraversal::version_record_locator(&store, &record);
        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists should succeed");
        assert!(exists);

        let bytes = RecordTraversal::read_record_bytes(&store, &locator)
            .await
            .expect("read should succeed");
        let loaded: FileRecord = serde_json::from_slice(&bytes).expect("should deserialize");
        assert_eq!(loaded, record);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_version_record_locators_empty_initially() {
        let store = make_store();
        let locators = RecordTraversal::list_version_record_locators(&store)
            .await
            .expect("list should succeed");
        assert!(locators.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_latest_record_locators_empty_initially() {
        let store = make_store();
        let locators = RecordTraversal::list_latest_record_locators(&store)
            .await
            .expect("list should succeed");
        assert!(locators.is_empty());
    }
}
