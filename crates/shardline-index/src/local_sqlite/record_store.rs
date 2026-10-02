use std::time::Duration;

use rusqlite::{OptionalExtension, params};
use shardline_protocol::unix_now_seconds_lossy;

use super::{
    LocalIndexStoreError, LocalRecordLocator, LocalRecordStore, RecordKind, i64_to_u64,
    record_not_found_error,
};
use crate::{
    FileRecord, RecordMutation, RecordStoreFuture, RecordTraversal, RepositoryRecordScope,
};

// Bound aggregate record bytes; one record may intrinsically exceed this budget.
const RECORD_BATCH_BYTES: usize = 1024 * 1024;

struct RecordScan {
    connection: super::index_store::ReadConnection,
    kind: RecordKind,
    after: Option<String>,
    repository_bounds: Option<(String, String)>,
}

impl RecordScan {
    fn new(
        store: &LocalRecordStore,
        kind: RecordKind,
        reservation: Option<super::index_store::CloseReservation>,
        repository: Option<RepositoryRecordScope>,
    ) -> Result<Self, LocalIndexStoreError> {
        let repository_bounds = repository
            .map(|repository| {
                let lower = crate::record_key::repository_record_scope_key(&repository);
                let upper = crate::hub::prefix_successor(&lower).ok_or_else(|| {
                    LocalIndexStoreError::BlockingTask(
                        "repository scope has no prefix upper bound".into(),
                    )
                })?;
                Ok::<_, LocalIndexStoreError>((lower, upper))
            })
            .transpose()?;
        let connection =
            super::index_store::ReadConnection::new(store.open_connection()?, reservation);
        if let Err(error) = connection.get()?.execute_batch("BEGIN DEFERRED") {
            connection.close();
            return Err(error.into());
        }
        Ok(Self {
            connection,
            kind,
            after: None,
            repository_bounds,
        })
    }

    fn abort(self) {
        self.connection.close();
    }

    fn finish(self) -> Result<(), LocalIndexStoreError> {
        let result = self
            .connection
            .get()?
            .execute_batch("COMMIT")
            .map_err(LocalIndexStoreError::from);
        self.connection.close();
        result
    }

    fn next_batch(
        &mut self,
        locators_only: bool,
    ) -> Result<
        Vec<Result<crate::StoredRecord<LocalRecordLocator>, LocalIndexStoreError>>,
        LocalIndexStoreError,
    > {
        let repository_predicate = if self.repository_bounds.is_some() {
            " AND scope_key >= ?2 AND scope_key < ?3 AND substr(scope_key, 1, length(?2)) = ?2"
        } else {
            ""
        };
        let predicate = match (self.after.is_some(), self.repository_bounds.is_some()) {
            (true, true) => " AND record_key > ?4",
            (true, false) => " AND record_key > ?2",
            (false, _) => "",
        };
        let columns = if locators_only {
            "record_key, record_kind, scope_key, file_id, content_hash"
        } else {
            "record_key, record_kind, scope_key, file_id, content_hash, record, updated_at_unix_seconds"
        };
        let sql = format!(
            "SELECT {columns} FROM shardline_file_records WHERE record_kind = ?1{repository_predicate}{predicate} ORDER BY record_key LIMIT {}",
            super::index_store::INVENTORY_BATCH_SIZE
        );
        let mut statement = self.connection.get()?.prepare(&sql)?;
        let mut parameters = vec![self.kind.as_str().to_owned()];
        if let Some((lower, upper)) = &self.repository_bounds {
            parameters.extend([lower.clone(), upper.clone()]);
        }
        parameters.extend(self.after.clone());
        let mut rows = statement.query(rusqlite::params_from_iter(parameters.iter()))?;
        let mut batch = Vec::with_capacity(super::index_store::INVENTORY_BATCH_SIZE);
        let mut batch_bytes = 0usize;
        while let Some(row) = rows.next()? {
            self.after = Some(row.get("record_key")?);
            let locator = super::helpers::local_record_locator_from_row(row)?;
            let entry = if locators_only {
                Ok(crate::StoredRecord {
                    locator,
                    bytes: Vec::new(),
                    modified_since_epoch: Duration::ZERO,
                })
            } else {
                // Keep errors in row order: the former traversal read/visited
                // earlier valid records before a later byte/timestamp error.
                (|| {
                    let bytes = super::helpers::read_sqlite_record_bytes(row.get_ref("record")?)?;
                    let modified_since_epoch =
                        Duration::from_secs(i64_to_u64(row.get("updated_at_unix_seconds")?)?);
                    Ok(crate::StoredRecord {
                        locator,
                        bytes,
                        modified_since_epoch,
                    })
                })()
            };
            batch_bytes =
                batch_bytes.saturating_add(entry.as_ref().map_or(0, |record| record.bytes.len()));
            batch.push(entry);
            if !locators_only && batch_bytes >= RECORD_BATCH_BYTES {
                break;
            }
        }
        Ok(batch)
    }
}

async fn visit_records<Visitor, VisitorError>(
    store: LocalRecordStore,
    kind: RecordKind,
    repository: Option<RepositoryRecordScope>,
    mut visitor: Visitor,
) -> Result<(), VisitorError>
where
    Visitor: FnMut(crate::StoredRecord<LocalRecordLocator>) -> Result<(), VisitorError>,
    LocalIndexStoreError: Into<VisitorError>,
    VisitorError: Send,
{
    let reservation = super::index_store::reserve_async_cursor().map_err(Into::into)?;
    let mut scan = tokio::task::spawn_blocking(move || {
        RecordScan::new(&store, kind, Some(reservation), repository)
    })
    .await
    .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))
    .map_err(Into::into)?
    .map_err(Into::into)?;
    for locators_only in [true, false] {
        scan.after = None;
        loop {
            let (next, batch) =
                tokio::task::spawn_blocking(move || match scan.next_batch(locators_only) {
                    Ok(batch) => Ok((scan, batch)),
                    Err(error) => {
                        scan.abort();
                        Err(error)
                    }
                })
                .await
                .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))
                .map_err(Into::into)?
                .map_err(Into::into)?;
            scan = next;
            if batch.is_empty() {
                break;
            }
            if !locators_only {
                for entry in batch {
                    let result = entry.map_err(Into::into).and_then(&mut visitor);
                    if let Err(error) = result {
                        tokio::task::spawn_blocking(move || scan.abort())
                            .await
                            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))
                            .map_err(Into::into)?;
                        return Err(error);
                    }
                }
            }
        }
    }
    tokio::task::spawn_blocking(move || scan.finish())
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))
        .map_err(Into::into)?
        .map_err(Into::into)
}

/// SQLite async record traversal shares a 64-cursor budget with index visitors,
/// including cursors being closed after cancellation. Exhaustion returns
/// `Io(WouldBlock)` immediately rather than blocking a nested visitor.
impl RecordTraversal for LocalRecordStore {
    type Error = LocalIndexStoreError;
    type Locator = LocalRecordLocator;

    fn visit_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(crate::StoredRecord<Self::Locator>) -> Result<(), VisitorError>
            + Send
            + 'operation,
        VisitorError: Send + 'operation,
    {
        let store = self.clone();
        Box::pin(async move { visit_records(store, RecordKind::Latest, None, visitor).await })
    }

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.list_record_locators(RecordKind::Latest))
                .await
                .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
        })
    }

    fn visit_repository_latest_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(crate::StoredRecord<Self::Locator>) -> Result<(), VisitorError>
            + Send
            + 'operation,
        VisitorError: Send + 'operation,
    {
        let store = self.clone();
        let repository = repository.clone();
        Box::pin(async move {
            visit_records(store, RecordKind::Latest, Some(repository), visitor).await
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
                store.list_repository_record_locators(RecordKind::Latest, &repository)
            })
            .await
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
        })
    }

    fn visit_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(crate::StoredRecord<Self::Locator>) -> Result<(), VisitorError>
            + Send
            + 'operation,
        VisitorError: Send + 'operation,
    {
        let store = self.clone();
        Box::pin(async move { visit_records(store, RecordKind::Version, None, visitor).await })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.list_record_locators(RecordKind::Version))
                .await
                .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
        })
    }

    fn visit_repository_version_records<'operation, Visitor, VisitorError>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
        visitor: Visitor,
    ) -> RecordStoreFuture<'operation, (), VisitorError>
    where
        Self: Sync,
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(crate::StoredRecord<Self::Locator>) -> Result<(), VisitorError>
            + Send
            + 'operation,
        VisitorError: Send + 'operation,
    {
        let store = self.clone();
        let repository = repository.clone();
        Box::pin(async move {
            visit_records(store, RecordKind::Version, Some(repository), visitor).await
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
                store.list_repository_record_locators(RecordKind::Version, &repository)
            })
            .await
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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
            .map_err(|e: tokio::task::JoinError| {
                LocalIndexStoreError::BlockingTask(e.to_string())
            })?
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        super::helpers::local_record_locator(RecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        super::helpers::local_record_locator(
            RecordKind::Version,
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
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
            .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
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
            storage_repr: crate::StorageRepresentation::FixedChunkV1,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn modified_since_epoch_returns_duration_for_existing_record() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_version_record(&store, &record)
            .await
            .expect("write should succeed");
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let duration = RecordTraversal::modified_since_epoch(&store, &locator)
            .await
            .expect("modified_since_epoch should succeed");
        assert!(
            duration > std::time::Duration::ZERO,
            "modified_since_epoch should be positive"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn modified_since_epoches_for_nonexistent_record() {
        let store = make_store();
        let record = sample_record();
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let result = RecordTraversal::modified_since_epoch(&store, &locator).await;
        assert!(
            result.is_err(),
            "modified_since_epoch should error for missing record"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_latest_record_bytes_returns_none_for_missing_record() {
        let store = make_store();
        let record = sample_record();
        let result = RecordTraversal::read_latest_record_bytes(&store, &record)
            .await
            .expect("read_latest_record_bytes should succeed");
        assert!(result.is_none(), "should be None for missing record");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_latest_record_bytes_returns_some_for_existing_record() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_latest_record(&store, &record)
            .await
            .expect("write should succeed");
        let result = RecordTraversal::read_latest_record_bytes(&store, &record)
            .await
            .expect("read_latest_record_bytes should succeed");
        assert!(result.is_some(), "should be Some for existing record");
        let loaded: FileRecord = serde_json::from_slice(&result.unwrap()).unwrap();
        assert_eq!(loaded.file_id, record.file_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_latest_record_locators_calls_visitor_for_each_record() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_latest_record(&store, &record)
            .await
            .expect("write should succeed");

        let mut visited = Vec::new();
        RecordTraversal::visit_latest_record_locators(&store, |locator| {
            visited.push(locator.file_id().to_owned());
            Ok::<(), LocalIndexStoreError>(())
        })
        .await
        .expect("visit should succeed");
        assert!(visited.contains(&record.file_id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_locator_exists_returns_false_for_missing() {
        let store = make_store();
        let record = sample_record();
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists check should succeed");
        assert!(!exists);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_record_locatores_for_missing() {
        let store = make_store();
        let record = sample_record();
        let locator = RecordTraversal::version_record_locator(&store, &record);
        let result = RecordMutation::delete_record_locator(&store, &locator).await;
        assert!(
            result.is_err(),
            "delete of non-existent locator should error"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_version_record_and_delete_roundtrip() {
        let store = make_store();
        let record = sample_record();
        RecordMutation::write_version_record(&store, &record)
            .await
            .expect("write should succeed");

        let locator = RecordTraversal::version_record_locator(&store, &record);
        assert!(
            RecordTraversal::record_locator_exists(&store, &locator)
                .await
                .unwrap()
        );

        RecordMutation::delete_record_locator(&store, &locator)
            .await
            .expect("delete should succeed");
        assert!(
            !RecordTraversal::record_locator_exists(&store, &locator)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prune_empty_latest_records_is_noop() {
        let store = make_store();
        let result = RecordMutation::prune_empty_latest_records(&store).await;
        assert!(result.is_ok());
    }
    #[test]
    fn streaming_records_current_thread_order_snapshot_and_error_cleanup() {
        let store = make_store();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            for i in 0..super::super::index_store::INVENTORY_BATCH_SIZE + 1 {
                let mut record = sample_record();
                record.file_id = format!("stream-{i:04}.bin");
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            let expected = RecordTraversal::list_latest_record_locators(&store).await.unwrap();
            let mut actual = Vec::new();
            RecordTraversal::visit_latest_records(&store, |entry| {
                if actual.is_empty() {
                    let writer = store.clone();
                    let handle = tokio::runtime::Handle::current();
                    let (sender, receiver) = std::sync::mpsc::channel();
                    std::thread::spawn(move || {
                        let mut record = sample_record();
                        record.file_id = "zz-added.bin".into();
                        sender.send(handle.block_on(writer.commit_file_version_metadata(&record))).unwrap();
                    });
                    receiver.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
                }
                let decoded: FileRecord = serde_json::from_slice(&entry.bytes).unwrap();
                assert_eq!(decoded.file_id, entry.locator.file_id);
                actual.push(entry.locator);
                Ok::<_, LocalIndexStoreError>(())
            }).await.unwrap();
            assert_eq!(actual, expected);
            let expected = RecordTraversal::list_version_record_locators(&store).await.unwrap();
            let mut actual = Vec::new();
            RecordTraversal::visit_version_records(&store, |entry| { actual.push(entry.locator); Ok::<_, LocalIndexStoreError>(()) }).await.unwrap();
            assert_eq!(actual, expected);
            let mut calls = 0;
            let error = RecordTraversal::visit_latest_records(&store, |_| { calls += 1; Err::<(), _>(LocalIndexStoreError::InvalidRecordKind) }).await.unwrap_err();
            assert!(matches!(error, LocalIndexStoreError::InvalidRecordKind));
            assert_eq!(calls, 1);
            assert_eq!(store.open_connection().unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let mut signal = Some(sender);
            let mut future = RecordTraversal::visit_latest_records(&store, |_| {
                if let Some(sender) = signal.take() { sender.send(()).unwrap(); }
                Ok::<_, LocalIndexStoreError>(())
            });
            tokio::select! { result = &mut future => panic!("visitor ended before cancellation: {result:?}"), result = receiver => result.unwrap() }
            drop(future);
            RecordTraversal::list_latest_record_locators(&store).await.unwrap();
            assert_eq!(store.open_connection().unwrap().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
        });
    }

    #[test]
    fn streaming_records_byte_budget_preserves_large_and_following_record() {
        let store = make_store();
        let mut first = sample_record();
        first.file_id = "a-large.bin".into();
        let mut second = sample_record();
        second.file_id = "b-small.bin".into();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime
            .block_on(store.commit_file_version_metadata(&first))
            .unwrap();
        runtime
            .block_on(store.commit_file_version_metadata(&second))
            .unwrap();
        let mut large = serde_json::to_vec(&first).unwrap();
        large.extend(std::iter::repeat_n(b' ', RECORD_BATCH_BYTES + 33));
        store.open_connection().unwrap().execute("UPDATE shardline_file_records SET record = ?1 WHERE record_kind = 'latest' AND file_id = ?2", params![large, first.file_id]).unwrap();
        let mut scan = RecordScan::new(&store, RecordKind::Latest, None, None).unwrap();
        let first_batch = scan.next_batch(false).unwrap();
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].as_ref().unwrap().bytes, large);
        let second_batch = scan.next_batch(false).unwrap();
        assert_eq!(second_batch.len(), 1);
        assert_eq!(
            second_batch[0].as_ref().unwrap().locator.file_id,
            second.file_id
        );
        assert!(scan.next_batch(false).unwrap().is_empty());
        drop(scan);
        runtime.block_on(async {
            let mut rows = Vec::new();
            RecordTraversal::visit_latest_records(&store, |row| {
                rows.push(row);
                Ok::<_, LocalIndexStoreError>(())
            })
            .await
            .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].bytes, large);
            assert_eq!(
                serde_json::from_slice::<FileRecord>(&rows[1].bytes).unwrap(),
                second
            );
        });
    }
    #[test]
    fn streaming_records_locator_prevalidation_and_ordered_timestamp_error() {
        let store = make_store();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            for i in 0..super::super::index_store::INVENTORY_BATCH_SIZE + 1 {
                let mut record = sample_record(); record.file_id = format!("ordered-{i:04}.bin");
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            let connection = store.open_connection().unwrap();
            let last = connection.query_row("SELECT record_key FROM shardline_file_records WHERE record_kind = 'latest' ORDER BY record_key DESC LIMIT 1", [], |row| row.get::<_, String>(0)).unwrap();
            connection.execute_batch("PRAGMA ignore_check_constraints = ON").unwrap();
            connection.execute("UPDATE shardline_file_records SET updated_at_unix_seconds = -1 WHERE record_key = ?1", [&last]).unwrap();
            let mut calls = 0;
            let error = RecordTraversal::visit_latest_records(&store, |_| { calls += 1; Ok::<_, LocalIndexStoreError>(()) }).await.unwrap_err();
            assert!(matches!(error, LocalIndexStoreError::IntegerOutOfRange(_)));
            assert_eq!(calls, super::super::index_store::INVENTORY_BATCH_SIZE);
            connection.execute("UPDATE shardline_file_records SET file_id = CAST(file_id AS BLOB) WHERE record_key = ?1", [&last]).unwrap();
            calls = 0;
            assert!(RecordTraversal::visit_latest_records(&store, |_| { calls += 1; Ok::<_, LocalIndexStoreError>(()) }).await.is_err());
            assert_eq!(calls, 0);
        });
    }
    #[test]
    fn repository_visitors_filter_literal_case_unicode_and_revision_scopes() {
        let store = make_store();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            for (owner, name, revision, id) in [
                ("team", "assets", Some("main"), "wanted"),
                ("team", "assets", Some("v2"), "wanted-v2"),
                ("TEAM", "ASSETS", Some("main"), "other-case"),
                ("t_%é", "r_%界", Some("main"), "literal"),
                ("tXXé", "rXX界", Some("main"), "other-pattern"),
            ] {
                let mut record = sample_record();
                record.file_id = id.into();
                record.repository_scope = Some(
                    shardline_protocol::RepositoryScope::new(
                        shardline_protocol::RepositoryProvider::GitLab,
                        owner,
                        name,
                        revision,
                    )
                    .unwrap(),
                );
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            for (owner, name, expected) in [
                ("team", "assets", vec!["wanted", "wanted-v2"]),
                ("t_%é", "r_%界", vec!["literal"]),
            ] {
                let scope = RepositoryRecordScope::new(
                    shardline_protocol::RepositoryProvider::GitLab,
                    owner,
                    name,
                );
                let latest = store
                    .list_repository_latest_record_locators(&scope)
                    .await
                    .unwrap();
                let versions = store
                    .list_repository_version_record_locators(&scope)
                    .await
                    .unwrap();
                let mut latest_ids = latest.iter().map(|l| l.file_id()).collect::<Vec<_>>();
                latest_ids.sort_unstable();
                let mut version_ids = versions.iter().map(|l| l.file_id()).collect::<Vec<_>>();
                version_ids.sort_unstable();
                assert_eq!(latest_ids, expected);
                assert_eq!(version_ids, expected);
                let mut seen = Vec::new();
                store
                    .visit_repository_latest_records(&scope, |entry| {
                        seen.push(entry.locator);
                        Ok::<_, LocalIndexStoreError>(())
                    })
                    .await
                    .unwrap();
                assert_eq!(seen, latest);
                seen.clear();
                store
                    .visit_repository_version_records(&scope, |entry| {
                        seen.push(entry.locator);
                        Ok::<_, LocalIndexStoreError>(())
                    })
                    .await
                    .unwrap();
                assert_eq!(seen, versions);
            }
        });
    }

    #[test]
    fn repository_visitors_keep_snapshot_across_batches_and_prevalidate_locators() {
        let store = make_store();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let scope = RepositoryRecordScope::new(shardline_protocol::RepositoryProvider::GitLab, "owner", "repo");
            for i in 0..super::super::index_store::INVENTORY_BATCH_SIZE + 1 {
                let mut record = sample_record(); record.file_id = format!("file-{i:04}");
                record.repository_scope = Some(shardline_protocol::RepositoryScope::new(
                    shardline_protocol::RepositoryProvider::GitLab, "owner", "repo", Some("main")).unwrap());
                store.commit_file_version_metadata(&record).await.unwrap();
            }
            let locators = store.list_repository_latest_record_locators(&scope).await.unwrap();
            let last = locators.last().unwrap();
            let connection = store.open_connection().unwrap();
            let original: Vec<u8> = connection.query_row("SELECT record FROM shardline_file_records WHERE record_key = ?1", [last.record_key()], |row| super::super::helpers::read_sqlite_record_bytes(row.get_ref(0)?)).unwrap();
            let mut seen = Vec::new();
            store.visit_repository_latest_records(&scope, |entry| {
                if seen.is_empty() {
                    store.open_connection().unwrap().execute("UPDATE shardline_file_records SET record = ?1 WHERE record_key = ?2", params![b"changed".as_slice(),last.record_key()]).unwrap();
                }
                if entry.locator == *last { assert_eq!(entry.bytes, original); }
                seen.push(entry.locator); Ok::<_,LocalIndexStoreError>(())
            }).await.unwrap();
            assert_eq!(seen, locators);
            connection.execute("UPDATE shardline_file_records SET record = ?1, file_id = CAST(file_id AS BLOB) WHERE record_key = ?2", params![original,last.record_key()]).unwrap();
            let mut calls = 0;
            assert!(store.visit_repository_latest_records(&scope, |_| { calls += 1; Ok::<_,LocalIndexStoreError>(()) }).await.is_err());
            assert_eq!(calls,0);
            // A callback error remains exact and stops delivery immediately.
            let mut calls = 0;
            let error = store.visit_repository_version_records(&scope, |_| { calls += 1; Err::<(),_>(LocalIndexStoreError::InvalidRecordKind) }).await.unwrap_err();
            assert!(matches!(error,LocalIndexStoreError::InvalidRecordKind)); assert_eq!(calls,1);
        });
    }
}
