//! Repository authorization for serialized Xorb payload bytes.

use std::collections::{BTreeMap, BTreeSet};

use shardline_index::RecordTraversal;
use shardline_protocol::{ByteRange, RepositoryScope};

use super::membership::LookupError;
use crate::{ServerError, record_store::parse_stored_file_record_bytes};

#[cfg(test)]
mod postgres;
#[cfg(test)]
mod scale;
#[cfg(test)]
mod short_circuit;
#[cfg(test)]
mod snapshot;

/// Discovery only: a nonempty candidate proves membership, not range access.
pub(crate) async fn first_xorb_coverage<Store>(
    store: &Store,
    hash: &str,
    scope: &RepositoryScope,
) -> Result<Coverage, ServerError>
where
    Store: RecordTraversal + Sync,
    Store::Error: Send,
    ServerError: From<Store::Error>,
{
    let Some(entry) = store.first_repository_hash_record(scope, hash).await? else {
        return Ok(Coverage::default());
    };
    let mut coverage = Coverage::default();
    coverage.include(&entry.bytes, scope, hash)?;
    if coverage.is_empty() {
        // A stale candidate cannot suppress a later authoritative grant.
        return repository_xorb_coverage(store, hash, scope, None).await;
    }
    Ok(coverage)
}

/// Uses candidate indexes only for discovery. The authoritative record must
/// authorize every requested byte, including gaps between adjacent file terms.
pub(crate) async fn repository_xorb_coverage<Store>(
    store: &Store,
    hash: &str,
    scope: &RepositoryScope,
    requested: Option<ByteRange>,
) -> Result<Coverage, ServerError>
where
    Store: RecordTraversal + Sync,
    Store::Error: Send,
    ServerError: From<Store::Error>,
{
    if let Some(range) = requested {
        let Some(entry) = store.first_repository_hash_record(scope, hash).await? else {
            return Ok(Coverage::default());
        };
        let mut first = Coverage::default();
        first.include(&entry.bytes, scope, hash)?;
        if first.covers(range) {
            return Ok(first);
        }
    }
    // A failed first-row proof contributes nothing to this traversal. Its
    // authoritative records belong to a new, internally consistent snapshot.
    let mut coverage = Coverage::default();
    let result = store
        .visit_repository_hash_records(scope, hash, |entry| {
            coverage
                .include(&entry.bytes, scope, hash)
                .map_err(LookupError::InvalidRecord)?;
            // Candidate membership is never sufficient: stop only after
            // authoritative file terms cover every byte actually requested.
            if requested.is_some_and(|range| coverage.covers(range)) {
                return Err(LookupError::Matched);
            }
            Ok(())
        })
        .await;
    match result {
        Ok(()) | Err(LookupError::Matched) => Ok(coverage),
        Err(LookupError::InvalidRecord(error)) => Err(error),
        Err(LookupError::Storage(error)) => Err(ServerError::from(error)),
    }
}

#[derive(Default)]
pub(crate) struct Coverage(BTreeMap<u64, u64>, BTreeSet<(u64, u64, u64, u64)>);

impl Coverage {
    fn include(
        &mut self,
        bytes: &[u8],
        scope: &RepositoryScope,
        hash: &str,
    ) -> Result<(), ServerError> {
        let record = parse_stored_file_record_bytes(bytes)?;
        self.include_record(&record, scope, hash);
        Ok(())
    }

    fn include_record(
        &mut self,
        record: &shardline_index::FileRecord,
        scope: &RepositoryScope,
        hash: &str,
    ) {
        if record.repository_scope.as_ref() == Some(scope) {
            for member in &record.chunks {
                if member.hash == hash && member.packed_end > member.packed_start {
                    self.insert(member.packed_start, member.packed_end.saturating_sub(1));
                    self.1.insert((
                        member.range_start,
                        member.range_end,
                        member.packed_start,
                        member.packed_end,
                    ));
                }
            }
        }
    }

    /// Candidate membership only; callers must separately prove requested bytes.
    pub(crate) fn from_file_record_members(
        record: &shardline_index::FileRecord,
        scope: &RepositoryScope,
        hash: &str,
    ) -> Option<Self> {
        let mut coverage = Self::default();
        coverage.include_record(record, scope, hash);
        (!coverage.is_empty()).then_some(coverage)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Native hashes bind decoded content, not compression. Published byte grants
    /// are valid only for the member layout that was verified at publication.
    /// Discard mismatching terms independently so an invalid current term cannot
    /// invalidate a complete historical grant from the same authoritative scope.
    pub(crate) fn for_layout(&self, chunks: &[shardline_xet_adapter::ValidatedXorbChunk]) -> Self {
        let mut verified = Self::default();
        for &(start, end, packed_start, packed_end) in &self.1 {
            let matches = (|| {
                let (Ok(start), Ok(end)) = (usize::try_from(start), usize::try_from(end)) else {
                    return false;
                };
                start < end
                    && chunks
                        .get(start)
                        .is_some_and(|member| member.packed_start() == packed_start)
                    && end
                        .checked_sub(1)
                        .and_then(|last| chunks.get(last))
                        .is_some_and(|member| member.packed_end() == packed_end)
            })();
            if matches {
                verified.insert(packed_start, packed_end.saturating_sub(1));
                verified.1.insert((start, end, packed_start, packed_end));
            }
        }
        verified
    }

    /// A complete payload owner may also read the footer. A partial owner may
    /// read only its serialized terms, never the shared container's other data.
    pub(crate) fn permits(&self, requested: ByteRange, packed_content_length: u64) -> bool {
        self.covers(requested)
            || packed_content_length
                .checked_sub(1)
                .and_then(|end| ByteRange::new(0, end).ok())
                .is_some_and(|payload| self.covers(payload))
    }

    fn insert(&mut self, mut start: u64, mut end: u64) {
        if let Some((&previous_start, &previous_end)) = self.0.range(..=start).next_back()
            && previous_end.saturating_add(1) >= start
        {
            start = previous_start;
            end = end.max(previous_end);
            self.0.remove(&previous_start);
        }
        while let Some((&next_start, &next_end)) = self.0.range(start..).next() {
            if next_start > end.saturating_add(1) {
                break;
            }
            end = end.max(next_end);
            self.0.remove(&next_start);
        }
        self.0.insert(start, end);
    }

    pub(crate) fn covers(&self, requested: ByteRange) -> bool {
        self.0
            .range(..=requested.start())
            .next_back()
            .is_some_and(|(_, end)| *end >= requested.end_inclusive())
    }
}

#[cfg(test)]
async fn repository_covers_xorb_range<Store>(
    store: &Store,
    hash: &str,
    scope: &RepositoryScope,
    requested: ByteRange,
) -> Result<bool, ServerError>
where
    Store: RecordTraversal + Sync,
    Store::Error: Send,
    ServerError: From<Store::Error>,
{
    Ok(
        repository_xorb_coverage(store, hash, scope, Some(requested))
            .await?
            .covers(requested),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Coverage;
    use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, StorageRepresentation};
    use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};

    #[test]
    fn published_member_grants_require_the_verified_serialized_layout() {
        use shardline_protocol::ShardlineHash;
        use shardline_xet_adapter::ValidatedXorbChunk;

        let chunks = [
            ValidatedXorbChunk::new(ShardlineHash::from_bytes([1; 32]), 0, 20, 0, 12),
            ValidatedXorbChunk::new(ShardlineHash::from_bytes([2; 32]), 20, 40, 12, 24),
        ];
        let mut coverage = Coverage::default();
        coverage.1.insert((0, 1, 0, 20));
        assert!(
            coverage
                .for_layout(&chunks)
                .covers(ByteRange::new(0, 19).unwrap())
        );
        for invalid in [(0, 1, 0, 40), (1, 2, 0, 40), (0, 3, 0, 40), (1, 1, 20, 20)] {
            let mut changed = Coverage::default();
            changed.1.insert(invalid);
            assert!(changed.for_layout(&chunks).is_empty());
        }
        coverage.1.insert((1, 2, 20, 40));
        assert!(
            coverage
                .for_layout(&chunks)
                .covers(ByteRange::new(0, 39).unwrap())
        );
        coverage.1.insert((0, 1, 0, 1));
        assert!(
            coverage
                .for_layout(&chunks)
                .covers(ByteRange::new(0, 39).unwrap())
        );
        let recompressed = [
            ValidatedXorbChunk::new(chunks[0].hash(), 0, 10, 0, 12),
            ValidatedXorbChunk::new(chunks[1].hash(), 10, 20, 12, 24),
        ];
        assert!(coverage.for_layout(&recompressed).is_empty());
    }

    #[tokio::test]
    async fn indexed_range_authorization_uses_exact_scope_and_historical_term_coverage() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
        let left =
            RepositoryScope::new(RepositoryProvider::GitHub, "left", "repo", Some("main")).unwrap();
        let right = RepositoryScope::new(RepositoryProvider::GitHub, "right", "repo", Some("main"))
            .unwrap();
        let hash = "a".repeat(64);
        let mut record = FileRecord {
            file_id: "first".into(),
            content_hash: "b".repeat(64),
            total_bytes: 4,
            chunk_size: 0,
            storage_repr: StorageRepresentation::WholeFileV1,
            repository_scope: Some(left.clone()),
            chunks: vec![FileChunkRecord {
                hash: hash.clone(),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 12,
            }],
        };
        store.commit_file_version_metadata(&record).await.unwrap();
        record.file_id = "second".into();
        record.chunks[0].packed_start = 24;
        record.chunks[0].packed_end = 36;
        store.commit_file_version_metadata(&record).await.unwrap();
        record.repository_scope = Some(right.clone());
        record.content_hash = "c".repeat(64);
        record.chunks[0].packed_start = 12;
        record.chunks[0].packed_end = 24;
        store.commit_file_version_metadata(&record).await.unwrap();
        let check = |start, end| ByteRange::new(start, end).unwrap();
        assert!(
            super::repository_covers_xorb_range(&store, &hash, &left, check(0, 11))
                .await
                .unwrap()
        );
        assert!(
            !super::repository_covers_xorb_range(&store, &hash, &left, check(0, 35))
                .await
                .unwrap(),
            "another repository must not fill the gap"
        );
        assert!(
            super::repository_covers_xorb_range(&store, &hash, &right, check(12, 23))
                .await
                .unwrap()
        );
        assert!(
            !super::repository_covers_xorb_range(&store, &hash, &right, check(0, 11))
                .await
                .unwrap()
        );
        record.repository_scope = Some(left.clone());
        store.commit_file_version_metadata(&record).await.unwrap();
        assert!(
            super::repository_covers_xorb_range(&store, &hash, &left, check(0, 35))
                .await
                .unwrap(),
            "adjacent authorized terms may cover a combined request"
        );
        assert!(
            !super::repository_covers_xorb_range(&store, &hash, &left, check(0, 36))
                .await
                .unwrap(),
            "container footer or other bytes need their own authority"
        );
    }

    #[test]
    fn coverage_merges_unordered_overlapping_and_adjacent_terms_without_hiding_gaps() {
        let mut coverage = Coverage::default();
        coverage.insert(20, 29);
        coverage.insert(0, 9);
        coverage.insert(25, 39);
        let requested = ByteRange::new(0, 39).unwrap();
        assert!(!coverage.covers(requested));
        coverage.insert(10, 19);
        assert!(coverage.covers(requested));
        assert_eq!(coverage.0.len(), 1);
        coverage.insert(2, 3);
        assert!(coverage.covers(requested));
    }

    #[test]
    fn coverage_handles_largest_inclusive_offset_without_overflow() {
        let mut coverage = Coverage::default();
        coverage.insert(u64::MAX, u64::MAX);
        coverage.insert(u64::MAX - 1, u64::MAX - 1);
        let requested = ByteRange::new(u64::MAX - 1, u64::MAX).unwrap();
        assert!(coverage.covers(requested));
        assert_eq!(coverage.0.len(), 1);
    }
}
