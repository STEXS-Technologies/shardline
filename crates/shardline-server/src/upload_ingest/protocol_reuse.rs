//! Connect raw-protocol ingest to the native deduplication machinery.
use std::collections::{HashMap, VecDeque};

use shardline_index::{FileChunkRecord, MAX_XET_DESCRIPTOR_CHUNKS};
use shardline_protocol::RepositoryScope;
use shardline_xet_core::merklehash::compute_data_hash;

use crate::{ServerBackend, ServerError, ShardMetadataLimits, admission::blocking_work};

pub(super) struct ProtocolChunkReuse {
    backend: ServerBackend,
    scope: Option<RepositoryScope>,
    members: VecDeque<HashMap<String, FileChunkRecord>>,
}

impl ProtocolChunkReuse {
    pub(super) const fn new(backend: ServerBackend, scope: Option<RepositoryScope>) -> Self {
        Self {
            backend,
            scope,
            members: VecDeque::new(),
        }
    }

    pub(super) fn find<'operation>(
        &'operation mut self,
        bytes: &'operation [u8],
        offset: u64,
    ) -> futures_util::future::BoxFuture<'operation, Result<Option<FileChunkRecord>, ServerError>>
    {
        Box::pin(async move {
            let hash = compute_data_hash(bytes).hex();
            if !self
                .members
                .iter()
                .any(|members| members.contains_key(&hash))
            {
                let wanted = hash.clone();
                let found =
                    self.backend
                        .with_scoped_dedup_sources(
                            &hash,
                            self.scope.as_ref(),
                            ShardMetadataLimits::default(),
                            move |sources| {
                                Ok(sources
                                    .into_iter()
                                    .find(|(descriptor, _, visible)| {
                                        descriptor.chunks.iter().zip(visible).any(
                                            |(member, allowed)| *allowed && member.hash == wanted,
                                        )
                                    })
                                    .map(|(descriptor, _, visible)| (descriptor, visible)))
                            },
                        )
                        .await;
                let (descriptor, visible) = match found {
                    Ok(Some(source)) => source,
                    Ok(None) | Err(ServerError::NotFound) => return Ok(None),
                    Err(error) => return Err(error),
                };
                // Candidate admission has committed. Full container reads must
                // not retain its database connection or deletion locks.
                // Final publication rechecks availability under reference locks.
                let objects = self.backend.object_store();
                let container_hash = descriptor.hash.clone();
                let verified = match blocking_work(self.backend.stream_work_pool(), move || {
                    Ok(shardline_xet_adapter::recover_xorb_descriptor(
                        &objects,
                        &container_hash,
                    )?)
                })
                .await
                {
                    Ok(verified) => verified,
                    Err(ServerError::NotFound) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if verified != descriptor {
                    return Err(ServerError::InvalidContentHash);
                }
                let mut members = HashMap::new();
                for (index, (member, allowed)) in descriptor.chunks.iter().zip(visible).enumerate()
                {
                    if allowed {
                        members.insert(
                            member.hash.clone(),
                            FileChunkRecord {
                                hash: descriptor.hash.clone(),
                                offset: 0,
                                length: member
                                    .unpacked_end
                                    .checked_sub(member.unpacked_start)
                                    .ok_or(ServerError::Overflow)?,
                                range_start: u64::try_from(index)?,
                                range_end: u64::try_from(index)?
                                    .checked_add(1)
                                    .ok_or(ServerError::Overflow)?,
                                packed_start: member.packed_start,
                                packed_end: member.packed_end,
                            },
                        );
                    }
                }
                // Retain alternating sources without increasing the former
                // one-descriptor member budget. The container cap also
                // bounds small maps and lookup work per chunk.
                while self.members.len() >= 8
                    || self
                        .members
                        .iter()
                        .map(HashMap::len)
                        .sum::<usize>()
                        .saturating_add(members.len())
                        > MAX_XET_DESCRIPTOR_CHUNKS
                {
                    self.members.pop_back();
                }
                self.members.push_front(members);
            }
            let Some(mut record) = self
                .members
                .iter()
                .find_map(|members| members.get(&hash))
                .cloned()
            else {
                return Ok(None);
            };
            if record.length != u64::try_from(bytes.len())? {
                return Err(ServerError::InvalidContentHash);
            }
            record.offset = offset;
            Ok(Some(record))
        })
    }

    pub(super) async fn register(
        &self,
        hash: &str,
        verified: Option<shardline_index::XetXorbDescriptor>,
    ) -> Result<(), ServerError> {
        let descriptor = match verified {
            Some(descriptor) => descriptor,
            None => {
                let objects = self.backend.object_store();
                let hash = hash.to_owned();
                blocking_work(self.backend.stream_work_pool(), move || {
                    Ok(shardline_xet_adapter::recover_xorb_descriptor(
                        &objects, &hash,
                    )?)
                })
                .await?
            }
        };
        self.backend
            .cache_verified_xorb_descriptor(&descriptor)
            .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )]
    use super::*;
    use axum::body::Bytes;
    use shardline_protocol::RepositoryProvider;
    use std::num::NonZeroUsize;

    async fn local_backend() -> (tempfile::TempDir, ServerBackend) {
        let root = tempfile::tempdir().unwrap();
        let local = crate::LocalBackend::new(
            root.path().to_owned(),
            "http://localhost".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .await
        .unwrap();
        (root, ServerBackend::Local(local))
    }

    #[tokio::test]
    async fn alternating_sources_stay_verified_until_bounded_cache_eviction() {
        let (root, backend) = local_backend().await;
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", None).unwrap();
        let ServerBackend::Local(local) = &backend else {
            panic!("local fixture");
        };
        let mut reuse = ProtocolChunkReuse::new(backend.clone(), Some(scope.clone()));
        let mut sources = Vec::new();
        for index in 0..9 {
            let bytes = Bytes::from(format!("independent immutable source container {index}"));
            let file = format!("source-{index}");
            let upload = local
                .upload_file(&file, bytes.clone(), Some(&scope))
                .await
                .unwrap();
            let record = local
                .file_record(&file, Some(&upload.content_hash), Some(&scope))
                .await
                .unwrap();
            let hash = record.chunks.first().unwrap().hash.clone();
            reuse.register(&hash, None).await.unwrap();
            assert_eq!(reuse.find(&bytes, 0).await.unwrap().unwrap().hash, hash);
            sources.push((bytes, hash));
            if index == 7 {
                // Remove candidate rows, not immutable content. Subsequent hits
                // must use verified upload-local entries rather than requerying
                // and rereading alternating source containers.
                let conn =
                    rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
                conn.execute("DELETE FROM shardline_xet_chunks", [])
                    .unwrap();
                let mut fresh = ProtocolChunkReuse::new(backend.clone(), Some(scope.clone()));
                assert!(fresh.find(&sources[0].0, 0).await.unwrap().is_none());
                for (offset, (bytes, hash)) in sources.iter().enumerate() {
                    let reused = reuse
                        .find(bytes, u64::try_from(offset).unwrap())
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(&reused.hash, hash);
                    assert_eq!(reused.offset, u64::try_from(offset).unwrap());
                }
            }
        }
        assert_eq!(reuse.members.len(), 8);
        assert!(reuse.members.iter().map(HashMap::len).sum::<usize>() <= MAX_XET_DESCRIPTOR_CHUNKS);
        assert!(reuse.find(&sources[0].0, 0).await.unwrap().is_none());
        for (bytes, hash) in sources.iter().skip(1) {
            assert_eq!(&reuse.find(bytes, 0).await.unwrap().unwrap().hash, hash);
        }
    }

    #[tokio::test]
    async fn reuse_obeys_native_member_scope_and_rejects_corrupt_source_bytes() {
        let (_root, backend) = local_backend().await;
        let ServerBackend::Local(local) = &backend else {
            panic!("local fixture");
        };
        let allowed =
            RepositoryScope::new(RepositoryProvider::GitHub, "alice", "repo", Some("main"))
                .unwrap();
        let denied =
            RepositoryScope::new(RepositoryProvider::GitHub, "bob", "repo", Some("main")).unwrap();
        let bytes = Bytes::from_static(b"verified native members require repository visibility");
        let upload = local
            .upload_file("source", bytes.clone(), Some(&allowed))
            .await
            .unwrap();
        let record = local
            .file_record("source", Some(&upload.content_hash), Some(&allowed))
            .await
            .unwrap();
        let hash = record.chunks.first().unwrap().hash.clone();
        let mut authorized = ProtocolChunkReuse::new(backend.clone(), Some(allowed.clone()));
        authorized.register(&hash, None).await.unwrap();
        let reused = authorized.find(&bytes, 123).await.unwrap().unwrap();
        assert_eq!(reused.hash, hash);
        assert_eq!(reused.offset, 123);
        let mut forbidden = ProtocolChunkReuse::new(backend.clone(), Some(denied));
        assert!(forbidden.find(&bytes, 0).await.unwrap().is_none());
        let objects = backend.object_store();
        let crate::ServerObjectStore::Local(store) = objects else {
            panic!("local fixture");
        };
        let key = crate::xet_adapter::xorb_object_key(&hash).unwrap();
        let path = store.path_for_key(&key);
        let mut corrupt = std::fs::read(&path).unwrap();
        corrupt[8] ^= 1;
        std::fs::write(path, corrupt).unwrap();
        let mut fresh = ProtocolChunkReuse::new(backend, Some(allowed));
        assert!(fresh.find(&bytes, 0).await.is_err());
    }
}
