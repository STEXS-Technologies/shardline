//! Upload-local reuse of fully verified chunks, including reads still in flight.
use std::{collections::VecDeque, sync::Arc};

use shardline_index::FileChunkRecord;
use tokio::sync::OnceCell;

use crate::ServerError;

pub(super) type VerifiedRead = Arc<OnceCell<Arc<Vec<u8>>>>;

#[derive(Default)]
pub(super) struct ReadCache {
    entries: VecDeque<(FileChunkRecord, VerifiedRead)>,
    bytes: u64,
}

impl ReadCache {
    pub(super) fn cell(
        &mut self,
        record: &FileChunkRecord,
        retain: bool,
    ) -> Result<Option<VerifiedRead>, ServerError> {
        if let Some((_, cell)) = self.entries.iter().find(|(existing, _)| {
            existing.hash == record.hash
                && existing.length == record.length
                && existing.packed_start == record.packed_start
                && existing.packed_end == record.packed_end
        }) {
            return Ok(Some(Arc::clone(cell)));
        }
        // An oversized chunk retains the original one-read behavior and must
        // not evict or bypass the cache's resident-byte bound.
        if !retain || record.length > super::MAX_READ_AHEAD_BYTES {
            return Ok(None);
        }
        let cell = Arc::new(OnceCell::new());
        while self.entries.len() >= super::MAX_READ_AHEAD_CHUNKS
            || self
                .bytes
                .checked_add(record.length)
                .ok_or(ServerError::Overflow)?
                > super::MAX_READ_AHEAD_BYTES
        {
            let (evicted, _) = self.entries.pop_front().ok_or(ServerError::Overflow)?;
            self.bytes = self
                .bytes
                .checked_sub(evicted.length)
                .ok_or(ServerError::Overflow)?;
        }
        self.bytes = self
            .bytes
            .checked_add(record.length)
            .ok_or(ServerError::Overflow)?;
        self.entries.push_back((record.clone(), Arc::clone(&cell)));
        Ok(Some(cell))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(index: usize, length: u64) -> FileChunkRecord {
        FileChunkRecord {
            hash: format!("{index:064x}"),
            offset: 0,
            length,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }
    }

    #[test]
    fn resident_chunks_obey_both_limits_and_unique_reads_are_not_retained() {
        let mut cache = ReadCache::default();
        assert!(cache.cell(&record(0, 64), false).unwrap().is_none());
        assert!(cache.entries.is_empty());
        for index in 0..40 {
            cache.cell(&record(index, 128 * 1024), true).unwrap();
            assert!(cache.entries.len() <= 32);
            assert!(cache.bytes <= 4 * 1024 * 1024);
        }
        assert!(cache.cell(&record(0, 128 * 1024), false).unwrap().is_none());
        let full = record(100, 4 * 1024 * 1024);
        let cell = cache.cell(&full, true).unwrap().unwrap();
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.bytes, full.length);
        assert!(
            cache
                .cell(&record(101, full.length + 1), true)
                .unwrap()
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &cell,
            &cache.cell(&full, false).unwrap().unwrap()
        ));
        for index in 0..40 {
            cache.cell(&record(index, 0), true).unwrap();
        }
        assert!(cache.entries.len() <= 32);
    }
}
