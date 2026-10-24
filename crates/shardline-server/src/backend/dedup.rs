//! Authenticated, repository-scoped native Xet deduplication replies.

use futures_util::StreamExt;
use std::{io, time::Duration};

use shardline_index::{PostgresIndexStore, RecordTraversal, XetXorbDescriptor, parse_xet_hash_hex};
use shardline_protocol::RepositoryScope;
use shardline_storage::AsyncObjectStore;
use shardline_xet_core::{
    merklehash::{MerkleHash, xorb_hash},
    metadata_shard::{
        serialize_scoped_keyed_dedup_shard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
};

use super::ServerBackend;
use crate::{ServerError, admission::blocking_work, record_store::parse_stored_file_record_bytes};

impl ServerBackend {
    pub(crate) async fn scoped_dedup_shard(
        &self,
        chunk_hash: &str,
        scope: Option<&RepositoryScope>,
        limits: crate::ShardMetadataLimits,
    ) -> Result<Vec<u8>, ServerError> {
        let mut after = None::<String>;
        let mut legacy_attempted = false;
        loop {
            let mut candidates = match self {
                Self::Local(backend) => {
                    backend
                        .record_store()
                        .xet_chunk_candidates_after(chunk_hash, scope, after.as_deref())
                        .await?
                }
                Self::Postgres(backend) => {
                    backend
                        .record_store()
                        .xet_chunk_candidates_after(chunk_hash, scope, after.as_deref())
                        .await?
                }
            };
            if candidates.is_empty() {
                if legacy_attempted {
                    return Err(ServerError::NotFound);
                }
                legacy_attempted = true;
                // Old deployments have retained shard mappings but no descriptor
                // cache. Rebuild one candidate from verified source bytes rather
                // than trusting the old shard's claimed membership.
                let (mut stream, length) = self.read_dedupe_shard_stream(chunk_hash).await?;
                const MAX_LEGACY_SHARD_BYTES: u64 = 64 * 1024 * 1024;
                if length > MAX_LEGACY_SHARD_BYTES {
                    return Err(invalid_descriptor());
                }
                let mut shard = Vec::new();
                while let Some(frame) = stream.next().await {
                    let frame = frame?;
                    if u64::try_from(shard.len())?.saturating_add(u64::try_from(frame.len())?)
                        > length
                    {
                        return Err(invalid_descriptor());
                    }
                    shard.extend_from_slice(&frame);
                }
                if u64::try_from(shard.len())? != length {
                    return Err(invalid_descriptor());
                }
                let (objects, pool) = match self {
                    Self::Local(backend) => {
                        (backend.object_store(), backend.stream_work_pool.clone())
                    }
                    Self::Postgres(backend) => {
                        (backend.object_store(), backend.stream_work_pool.clone())
                    }
                };
                let lookup_hash = chunk_hash.to_owned();
                let claims = blocking_work(&pool, move || {
                    Ok(shardline_xet_adapter::dedup_xorb_candidates(
                        &shard,
                        &lookup_hash,
                        limits,
                    )?)
                })
                .await?;
                let mut authorized = Vec::new();
                for claimed in claims {
                    let allowed = match (self, scope) {
                        (Self::Local(backend), Some(scope)) => {
                            legacy_member_visible(
                                backend.record_store(),
                                &claimed,
                                chunk_hash,
                                scope,
                            )
                            .await?
                        }
                        (Self::Postgres(backend), Some(scope)) => {
                            legacy_member_visible(
                                backend.record_store(),
                                &claimed,
                                chunk_hash,
                                scope,
                            )
                            .await?
                        }
                        (_, None) => true,
                    };
                    if allowed {
                        authorized.push(claimed);
                    }
                }
                if authorized.is_empty() {
                    return Err(ServerError::NotFound);
                }
                let hash = chunk_hash.to_owned();
                let descriptor = blocking_work(&pool, move || {
                    Ok(
                        shardline_xet_adapter::recover_dedup_xorb_descriptor_from_candidates(
                            &objects,
                            &authorized,
                            &hash,
                        )?,
                    )
                })
                .await?;
                if let Some(descriptor) = descriptor {
                    match self {
                        Self::Local(backend) => {
                            backend
                                .record_store()
                                .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
                                .await?
                        }
                        Self::Postgres(backend) => {
                            backend
                                .record_store()
                                .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
                                .await?
                        }
                    }
                    candidates.push(descriptor);
                }
            }
            // Advance by the final database row, even when every source on a page
            // is stale or the requested member is outside this repository's terms.
            // Never let an arbitrary first-page limit decide dedup eligibility.
            let next = candidates.last().map(|descriptor| descriptor.hash.clone());
            for descriptor in candidates {
                // The candidate index and serialized offsets are not trusted. Native
                // root verification authenticates the chunk identities and lengths.
                let info = descriptor_info(&descriptor)?;
                let visible = match (self, scope) {
                    (Self::Local(backend), Some(scope)) => {
                        visibility(backend.record_store(), &descriptor, scope).await?
                    }
                    (Self::Postgres(backend), Some(scope)) => {
                        visibility(backend.record_store(), &descriptor, scope).await?
                    }
                    (_, None) => vec![true; descriptor.chunks.len()],
                };
                if !descriptor
                    .chunks
                    .iter()
                    .zip(&visible)
                    .any(|(chunk, allowed)| *allowed && chunk.hash == chunk_hash)
                {
                    continue;
                }
                let object_key = crate::xet_adapter::xorb_object_key(&descriptor.hash)?;
                let (objects, pool) = match self {
                    Self::Local(backend) => {
                        (backend.object_store(), backend.stream_work_pool.clone())
                    }
                    Self::Postgres(backend) => {
                        (backend.object_store(), backend.stream_work_pool.clone())
                    }
                };
                // Do not advertise an unresolved remote deletion, even if a late
                // object-store response still reports the object as present.
                let mut delete_guard = match self {
                    Self::Postgres(backend) => {
                        let mut transaction = backend
                            .index_store()
                            .pool()
                            .begin()
                            .await
                            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
                        PostgresIndexStore::require_remote_delete_settled(
                            &mut transaction,
                            &object_key,
                        )
                        .await?;
                        Some(transaction)
                    }
                    Self::Local(_) => None,
                };
                let Some(metadata) = AsyncObjectStore::metadata(&objects, &object_key).await?
                else {
                    continue;
                };
                if metadata.length() != descriptor.total_bytes {
                    return Err(invalid_descriptor());
                }
                let bytes = blocking_work(&pool, move || {
                    let mut public_key = [0_u8; 32];
                    let mut private_key = [0_u8; 32];
                    getrandom::fill(&mut public_key)
                        .and_then(|()| getrandom::fill(&mut private_key))
                        .map_err(|_error| {
                            ServerError::Io(io::Error::other("deduplication entropy unavailable"))
                        })?;
                    serialize_scoped_keyed_dedup_shard(
                        &[info],
                        &[visible],
                        public_key.into(),
                        private_key.into(),
                        Duration::from_secs(900),
                    )
                    .map_err(|_error| invalid_descriptor())
                })
                .await?;
                if let Some(transaction) = delete_guard.take() {
                    transaction
                        .commit()
                        .await
                        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
                }
                return Ok(bytes);
            }
            if legacy_attempted {
                return Err(ServerError::NotFound);
            }
            after = next;
        }
    }
}

fn invalid_descriptor() -> ServerError {
    ServerError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid native Xet deduplication descriptor",
    ))
}

/// This only admits source verification. The resulting descriptor and final
/// repository coverage are still verified before any dedup reply is returned.
async fn legacy_member_visible<Store: RecordTraversal + Sync>(
    store: &Store,
    claimed: &MDBXorbInfo,
    chunk_hash: &str,
    scope: &RepositoryScope,
) -> Result<bool, ServerError>
where
    ServerError: From<Store::Error>,
{
    let expected = MerkleHash::from(*parse_xet_hash_hex(chunk_hash)?.as_bytes());
    let positions = claimed
        .chunks
        .iter()
        .enumerate()
        .filter(|(_, chunk)| chunk.chunk_hash == expected)
        .map(|(position, _)| u64::try_from(position))
        .collect::<Result<Vec<_>, _>>()?;
    let hash = claimed.metadata.xorb_hash.hex();
    let mut visible = false;
    store
        .visit_repository_hash_records(scope, &hash, |entry| {
            let record = parse_stored_file_record_bytes(&entry.bytes)?;
            if record.repository_scope.as_ref() == Some(scope) {
                visible |= record.chunks.iter().any(|term| {
                    term.hash == hash
                        && positions.iter().any(|position| {
                            term.range_start <= *position && *position < term.range_end
                        })
                });
            }
            Ok::<_, ServerError>(())
        })
        .await?;
    Ok(visible)
}

pub(super) fn descriptor_info(descriptor: &XetXorbDescriptor) -> Result<MDBXorbInfo, ServerError> {
    if !descriptor.has_valid_structure() {
        return Err(invalid_descriptor());
    }
    let identity = MerkleHash::from(*parse_xet_hash_hex(&descriptor.hash)?.as_bytes());
    let chunks = descriptor
        .chunks
        .iter()
        .map(|chunk| {
            Ok((
                MerkleHash::from(*parse_xet_hash_hex(&chunk.hash)?.as_bytes()),
                chunk
                    .unpacked_end
                    .checked_sub(chunk.unpacked_start)
                    .ok_or_else(invalid_descriptor)?,
            ))
        })
        .collect::<Result<Vec<_>, ServerError>>()?;
    if xorb_hash(&chunks) != identity {
        return Err(invalid_descriptor());
    }
    let mut metadata = XorbChunkSequenceHeader::new(
        identity,
        u64::try_from(chunks.len())?,
        descriptor
            .chunks
            .last()
            .ok_or_else(invalid_descriptor)?
            .unpacked_end,
    );
    metadata.num_bytes_on_disk = descriptor.total_bytes;
    let entries = chunks
        .into_iter()
        .zip(&descriptor.chunks)
        .map(|((hash, length), chunk)| {
            XorbChunkSequenceEntry::new(hash, length, chunk.unpacked_start)
        })
        .collect();
    Ok(MDBXorbInfo {
        metadata,
        chunks: entries,
    })
}

async fn visibility<Store: RecordTraversal + Sync>(
    store: &Store,
    descriptor: &XetXorbDescriptor,
    scope: &RepositoryScope,
) -> Result<Vec<bool>, ServerError>
where
    ServerError: From<Store::Error>,
{
    let mut visible = vec![false; descriptor.chunks.len()];
    store
        .visit_repository_hash_records(scope, &descriptor.hash, |entry| {
            let record = parse_stored_file_record_bytes(&entry.bytes)?;
            if record.repository_scope.as_ref() != Some(scope) {
                return Ok::<_, ServerError>(());
            }
            for term in record
                .chunks
                .iter()
                .filter(|term| term.hash == descriptor.hash)
            {
                let start = usize::try_from(term.range_start)?;
                let end = usize::try_from(term.range_end)?;
                let members = descriptor
                    .chunks
                    .get(start..end)
                    .filter(|members| !members.is_empty())
                    .ok_or_else(invalid_descriptor)?;
                let length = members
                    .last()
                    .ok_or_else(invalid_descriptor)?
                    .unpacked_end
                    .checked_sub(
                        members
                            .first()
                            .ok_or_else(invalid_descriptor)?
                            .unpacked_start,
                    )
                    .ok_or_else(invalid_descriptor)?;
                if length != term.length {
                    return Err(invalid_descriptor());
                }
                visible
                    .get_mut(start..end)
                    .ok_or_else(invalid_descriptor)?
                    .fill(true);
            }
            Ok(())
        })
        .await?;
    Ok(visible)
}
