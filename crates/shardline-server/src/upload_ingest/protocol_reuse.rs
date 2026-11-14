//! Connect raw-protocol ingest to the native deduplication machinery.
use std::collections::HashMap;

use shardline_index::FileChunkRecord;
use shardline_protocol::RepositoryScope;
use shardline_xet_core::merklehash::compute_data_hash;

use crate::{ServerBackend, ServerError, ShardMetadataLimits, admission::blocking_work};

pub(super) struct ProtocolChunkReuse {
    backend: ServerBackend,
    scope: Option<RepositoryScope>,
    members: HashMap<String, FileChunkRecord>,
}

impl ProtocolChunkReuse {
    pub(super) fn new(backend: ServerBackend, scope: Option<RepositoryScope>) -> Self {
        Self {
            backend,
            scope,
            members: HashMap::new(),
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
            if !self.members.contains_key(&hash) {
                let objects = self.backend.object_store();
                let wanted = hash.clone();
                let found = self
                    .backend
                    .with_scoped_dedup_sources(
                        &hash,
                        self.scope.as_ref(),
                        ShardMetadataLimits::default(),
                        move |sources| {
                            for (descriptor, _, visible) in sources {
                                if !descriptor
                                    .chunks
                                    .iter()
                                    .zip(&visible)
                                    .any(|(member, allowed)| *allowed && member.hash == wanted)
                                {
                                    continue;
                                }
                                // The native lookup establishes authorization and identity;
                                // existing source verification authenticates actual storage
                                // offsets before a raw-protocol record can reuse them.
                                let verified = shardline_xet_adapter::recover_xorb_descriptor(
                                    &objects,
                                    &descriptor.hash,
                                )?;
                                if verified != descriptor {
                                    return Err(ServerError::InvalidContentHash);
                                }
                                return Ok(Some((verified, visible)));
                            }
                            Ok(None)
                        },
                    )
                    .await;
                match found {
                    Ok(Some((descriptor, visible))) => {
                        // One native container bounds retained lookup metadata.
                        self.members.clear();
                        for (index, (member, allowed)) in
                            descriptor.chunks.iter().zip(visible).enumerate()
                        {
                            if allowed {
                                self.members.insert(
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
                    }
                    Ok(None) | Err(ServerError::NotFound) => return Ok(None),
                    Err(error) => return Err(error),
                }
            }
            let Some(mut record) = self.members.get(&hash).cloned() else {
                return Ok(None);
            };
            if record.length != u64::try_from(bytes.len())? {
                return Err(ServerError::InvalidContentHash);
            }
            record.offset = offset;
            Ok(Some(record))
        })
    }

    pub(super) async fn register(&self, hash: &str) -> Result<(), ServerError> {
        let objects = self.backend.object_store();
        let hash = hash.to_owned();
        let descriptor = blocking_work(self.backend.stream_work_pool(), move || {
            Ok(shardline_xet_adapter::recover_xorb_descriptor(
                &objects, &hash,
            )?)
        })
        .await?;
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

    #[tokio::test]
    async fn reuse_obeys_native_member_scope_and_rejects_corrupt_source_bytes() {
        let root = tempfile::tempdir().unwrap();
        let local = crate::LocalBackend::new(
            root.path().to_owned(),
            "http://localhost".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .await
        .unwrap();
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
        let backend = ServerBackend::Local(local);
        let mut authorized = ProtocolChunkReuse::new(backend.clone(), Some(allowed.clone()));
        authorized.register(&hash).await.unwrap();
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
