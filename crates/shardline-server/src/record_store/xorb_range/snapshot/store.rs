//! Deterministic storage snapshots using real SQLite traversal implementations.

use shardline_index::{
    FileRecord, LocalIndexStoreError, LocalRecordStore, RecordStoreFuture, RecordTraversal,
    RepositoryRecordScope, StoredRecord,
};
use shardline_protocol::RepositoryScope;
use std::time::Duration;

pub(super) struct Snapshots {
    pub(super) first: LocalRecordStore,
    pub(super) fallback: LocalRecordStore,
}

impl RecordTraversal for Snapshots {
    type Error = LocalIndexStoreError;
    type Locator = <LocalRecordStore as RecordTraversal>::Locator;

    fn first_repository_hash_record<'operation>(
        &'operation self,
        repository: &'operation RepositoryScope,
        hash: &'operation str,
    ) -> RecordStoreFuture<'operation, Option<StoredRecord<Self::Locator>>, Self::Error>
    where
        Self::Error: Send + 'operation,
    {
        self.first.first_repository_hash_record(repository, hash)
    }

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        self.fallback.list_latest_record_locators()
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        self.fallback
            .list_repository_latest_record_locators(repository)
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        self.fallback.list_version_record_locators()
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        self.fallback
            .list_repository_version_record_locators(repository)
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        self.fallback.read_record_bytes(locator)
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        self.fallback.read_latest_record_bytes(record)
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        self.fallback.record_locator_exists(locator)
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        self.fallback.modified_since_epoch(locator)
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        self.fallback.latest_record_locator(record)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        self.fallback.version_record_locator(record)
    }
}
