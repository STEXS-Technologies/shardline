//! Repository-authorized native delta metadata; never exposes arbitrary Xorb members.

use std::{io, ops::Range};

use shardline_index::{PostgresIndexStore, parse_xet_hash_hex};
use shardline_protocol::RepositoryScope;
use shardline_storage::AsyncObjectStore;
use shardline_xet_core::merklehash::{
    MerkleHash,
    delta::{FileChunkHashesResponse, file_chunk_hashes},
    file_hash,
};

use super::ServerBackend;
use crate::{ServerError, admission::blocking_work};

fn invalid() -> ServerError {
    io::Error::new(io::ErrorKind::InvalidData, "invalid native delta metadata").into()
}

impl ServerBackend {
    pub(crate) async fn native_delta(
        &self,
        file_id: &str,
        scope: Option<&RepositoryScope>,
        dirty: Vec<Range<u64>>,
    ) -> Result<FileChunkHashesResponse, ServerError> {
        let record = match self {
            Self::Local(backend) => backend.file_record(file_id, None, scope).await?,
            Self::Postgres(backend) => backend.read_record(file_id, None, scope).await?,
        };
        let (objects, pool) = match self {
            Self::Local(backend) => (backend.object_store(), backend.stream_work_pool.clone()),
            Self::Postgres(backend) => (backend.object_store(), backend.stream_work_pool.clone()),
        };
        let mut chunks = Vec::new();
        let mut segments = Vec::new();
        let mut length = 0u64;
        for term in &record.chunks {
            if term.offset != length {
                return Err(invalid());
            }
            let descriptor = match self {
                Self::Local(backend) => {
                    backend
                        .record_store()
                        .xet_xorb_descriptor(&term.hash)
                        .await?
                }
                Self::Postgres(backend) => {
                    backend
                        .record_store()
                        .xet_xorb_descriptor(&term.hash)
                        .await?
                }
            };
            let object_key = crate::xet_adapter::xorb_object_key(&term.hash)?;
            let mut guard = match self {
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
            let descriptor = match descriptor {
                Some(descriptor) => descriptor,
                None => {
                    // Released data may predate the derived descriptor index.
                    // Recover from fully verified source bytes, never client claims.
                    let source = objects.clone();
                    let hash = term.hash.clone();
                    let recovered = blocking_work(&pool, move || {
                        Ok(shardline_xet_adapter::recover_xorb_descriptor(
                            &source, &hash,
                        )?)
                    })
                    .await?;
                    match self {
                        Self::Local(backend) => {
                            backend
                                .record_store()
                                .cache_xet_xorb_descriptors(std::slice::from_ref(&recovered))
                                .await?
                        }
                        Self::Postgres(backend) => {
                            backend
                                .record_store()
                                .cache_xet_xorb_descriptors(std::slice::from_ref(&recovered))
                                .await?
                        }
                    }
                    recovered
                }
            };
            let metadata = AsyncObjectStore::metadata(&objects, &object_key)
                .await?
                .ok_or(ServerError::NotFound)?;
            if metadata.length() != descriptor.total_bytes || descriptor.hash != term.hash {
                return Err(invalid());
            }
            let start = usize::try_from(term.range_start)?;
            let end = usize::try_from(term.range_end)?;
            let expected_length = term.length;
            let selected = blocking_work(&pool, move || {
                let info = super::dedup::descriptor_info(&descriptor)?;
                let selected = info
                    .chunks
                    .get(start..end)
                    .filter(|slice| !slice.is_empty())
                    .ok_or_else(invalid)?;
                let selected: Vec<_> = selected
                    .iter()
                    .map(|chunk| (chunk.chunk_hash, chunk.unpacked_segment_bytes))
                    .collect();
                let size = selected
                    .iter()
                    .try_fold(0u64, |sum, (_, size)| sum.checked_add(*size))
                    .ok_or_else(invalid)?;
                if size != expected_length {
                    return Err(invalid());
                }
                Ok(selected)
            })
            .await?;
            let segment_start = chunks.len();
            // Bound repeated terms as well as independently stored descriptors.
            if segment_start.saturating_add(selected.len()) > 1_000_000 {
                return Err(ServerError::WorkQueueSaturated);
            }
            chunks.extend(selected);
            segments.push(segment_start..chunks.len());
            length = length.checked_add(term.length).ok_or_else(invalid)?;
            if let Some(transaction) = guard.take() {
                transaction
                    .commit()
                    .await
                    .map_err(shardline_index::PostgresMetadataStoreError::from)?;
            }
        }
        if length != record.total_bytes {
            return Err(invalid());
        }
        let identity = MerkleHash::from(*parse_xet_hash_hex(file_id)?.as_bytes());
        blocking_work(&pool, move || {
            if file_hash(&chunks) != identity {
                return Err(invalid());
            }
            file_chunk_hashes(&chunks, &segments, &dirty)
                .map_err(|_error| ServerError::InvalidContentHash)
        })
        .await
    }
}
