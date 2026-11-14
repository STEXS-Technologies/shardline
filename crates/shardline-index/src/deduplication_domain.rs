//! Reuse authorization independent of a file record's storage location.
use serde::{Deserialize, Serialize};
use shardline_protocol::RepositoryScope;

/// Exact repository/ref boundary for chunk reuse, or the public global domain.
///
/// This is persisted with authoritative records; it is not an access token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DeduplicationDomain(String);

impl DeduplicationDomain {
    /// Derives the existing canonical membership key. `None` means public/global,
    /// never an unrestricted search across repositories.
    #[must_use]
    pub fn from_scope(scope: Option<&RepositoryScope>) -> Self {
        Self(crate::record_key::repository_scope_key(scope))
    }

    /// Returns the canonical key used by the derived membership index.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
    use super::*;
    use crate::{FileRecord, LocalRecordStore, RecordTraversal, StorageRepresentation};
    use shardline_protocol::RepositoryProvider;

    #[test]
    fn explicit_reuse_domain_roundtrips_without_relocating_protocol_records() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main"))
            .unwrap();
        let mut record = FileRecord {
            deduplication_domain: None,
            file_id: "protocol-object-contract".to_owned(),
            content_hash: "ab".repeat(32),
            total_bytes: 0,
            chunk_size: 0,
            storage_repr: StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: Vec::new(),
        };
        let store = LocalRecordStore::open(std::path::PathBuf::from("domain-locator-contract"));
        let latest = store.latest_record_locator(&record);
        let version = store.version_record_locator(&record);
        let legacy_json = serde_json::to_vec(&record).unwrap();
        assert!(!String::from_utf8_lossy(&legacy_json).contains("deduplication_domain"));
        assert_eq!(
            serde_json::from_slice::<FileRecord>(&legacy_json).unwrap(),
            record
        );
        assert_eq!(record.reuse_domain(), DeduplicationDomain::from_scope(None));

        record.deduplication_domain = Some(DeduplicationDomain::from_scope(Some(&scope)));
        assert_eq!(
            latest.record_key(),
            store.latest_record_locator(&record).record_key()
        );
        assert_eq!(
            version.record_key(),
            store.version_record_locator(&record).record_key()
        );
        assert!(record.repository_scope.is_none());
        assert_eq!(
            record.reuse_domain(),
            DeduplicationDomain::from_scope(Some(&scope))
        );
        let encoded = serde_json::to_vec(&record).unwrap();
        assert_eq!(
            serde_json::from_slice::<FileRecord>(&encoded).unwrap(),
            record
        );

        // Native records retain their existing domain without extra metadata.
        record.deduplication_domain = None;
        record.repository_scope = Some(scope.clone());
        assert_eq!(
            record.reuse_domain(),
            DeduplicationDomain::from_scope(Some(&scope))
        );
    }

    #[test]
    fn repository_provider_owner_name_and_ref_each_separate_domains() {
        let baseline =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main"))
                .unwrap();
        let expected = DeduplicationDomain::from_scope(Some(&baseline));
        assert_ne!(expected, DeduplicationDomain::from_scope(None));
        for (provider, owner, name, revision) in [
            (RepositoryProvider::GitLab, "alice", "repo", Some("main")),
            (RepositoryProvider::GitHub, "bob", "repo", Some("main")),
            (RepositoryProvider::GitHub, "alice", "other", Some("main")),
            (RepositoryProvider::GitHub, "alice", "repo", Some("feature")),
            (RepositoryProvider::GitHub, "alice", "repo", None),
        ] {
            let other = RepositoryScope::new(provider, owner, name, revision).unwrap();
            assert_ne!(expected, DeduplicationDomain::from_scope(Some(&other)));
        }
    }
    async fn candidates<Store: RecordTraversal + Sync>(
        store: &Store,
        domain: &DeduplicationDomain,
        hash: &str,
    ) -> usize
    where
        Store::Error: std::fmt::Debug + Send,
    {
        let mut count = 0_usize;
        store
            .visit_deduplication_hash_records(domain, hash, |stored| {
                let record: FileRecord = serde_json::from_slice(&stored.bytes).unwrap();
                assert_eq!(&record.reuse_domain(), domain);
                count = count.saturating_add(1);
                Ok::<_, Store::Error>(())
            })
            .await
            .unwrap();
        count
    }

    async fn domain_membership_contract<Store: crate::RecordMutation + Sync>(store: &Store)
    where
        Store::Error: std::fmt::Debug + Send,
    {
        let first = RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main"))
            .unwrap();
        let second =
            RepositoryScope::new(RepositoryProvider::GitHub, "bob", "repo", Some("main")).unwrap();
        let a = DeduplicationDomain::from_scope(Some(&first));
        let b = DeduplicationDomain::from_scope(Some(&second));
        let global = DeduplicationDomain::from_scope(None);
        let hash = "a".repeat(64);
        let mut record = FileRecord {
            deduplication_domain: Some(a.clone()),
            file_id: "protocol-object-domain-membership".into(),
            content_hash: "b".repeat(64),
            total_bytes: 1,
            chunk_size: 0,
            storage_repr: StorageRepresentation::XorbCdcV1,
            repository_scope: None,
            chunks: vec![crate::FileChunkRecord {
                hash: hash.clone(),
                offset: 0,
                length: 1,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 1,
            }],
        };
        store.write_version_record(&record).await.unwrap();
        store.write_latest_record(&record).await.unwrap();
        assert_eq!(candidates(store, &a, &hash).await, 2);
        assert_eq!(candidates(store, &b, &hash).await, 0);
        assert_eq!(candidates(store, &global, &hash).await, 0);
        let latest = store.latest_record_locator(&record);
        let version = store.version_record_locator(&record);
        record.deduplication_domain = Some(b.clone());
        store.write_version_record(&record).await.unwrap();
        store.write_latest_record(&record).await.unwrap();
        assert_eq!(candidates(store, &a, &hash).await, 0);
        assert_eq!(candidates(store, &b, &hash).await, 2);
        store.delete_record_locator(&latest).await.unwrap();
        store.delete_record_locator(&version).await.unwrap();
        assert_eq!(candidates(store, &b, &hash).await, 0);
    }

    #[tokio::test]
    async fn sqlite_domain_membership_tracks_publication_replacement_and_deletion() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
        domain_membership_contract(&store).await;
    }

    #[tokio::test]
    async fn postgres_domain_membership_tracks_publication_replacement_and_deletion() {
        let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
            return;
        };
        let store = crate::PostgresRecordStore::new(pool);
        domain_membership_contract(&store).await;
    }
}
