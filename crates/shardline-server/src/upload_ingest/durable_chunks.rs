//! Bounded, ordered read-ahead for verified durable upload chunks.
use std::sync::Arc;

use futures_util::{StreamExt, future::BoxFuture, stream::FuturesOrdered};
use shardline_index::{FileChunkRecord, xet_hash_hex_string};
use shardline_protocol::ByteRange;
use shardline_storage::AsyncObjectStore;

use crate::{ServerError, error::ObjectStoreError, object_store::ServerObjectStore};

const MAX_READ_AHEAD_BYTES: u64 = 4 * 1024 * 1024;
const MAX_READ_AHEAD_CHUNKS: usize = 32;
type ChunkRead = BoxFuture<'static, Result<(Vec<u8>, u64), ServerError>>;

mod read_cache;

pub(super) struct DurableUploadChunks {
    store: ServerObjectStore,
    pending: FuturesOrdered<ChunkRead>,
    next_record: usize,
    pending_bytes: u64,
    parallelism: usize,
    cache: read_cache::ReadCache,
}

impl DurableUploadChunks {
    pub(super) fn new(store: ServerObjectStore, parallelism: usize) -> Self {
        Self {
            store,
            pending: FuturesOrdered::new(),
            next_record: 0,
            pending_bytes: 0,
            parallelism: parallelism.clamp(1, MAX_READ_AHEAD_CHUNKS),
            cache: read_cache::ReadCache::default(),
        }
    }

    pub(super) async fn next(
        &mut self,
        records: &[FileChunkRecord],
    ) -> Result<Option<(Vec<u8>, u64)>, ServerError> {
        while self.pending.len() < self.parallelism {
            let Some(record) = records.get(self.next_record) else {
                break;
            };
            let bytes = self
                .pending_bytes
                .checked_add(record.length)
                .ok_or(ServerError::Overflow)?;
            // A single large chunk retains the previous one-chunk behavior.
            // Additional reads obey both the byte and request-count budgets.
            if !self.pending.is_empty() && bytes > MAX_READ_AHEAD_BYTES {
                break;
            }
            // Cache only chunks repeated in the bounded look-ahead window.
            // Unique content keeps its original allocation and read path.
            let retain = records
                .iter()
                .skip(self.next_record)
                .take(MAX_READ_AHEAD_CHUNKS)
                .skip(1)
                .any(|next| {
                    next.hash == record.hash
                        && next.length == record.length
                        && next.packed_start == record.packed_start
                        && next.packed_end == record.packed_end
                });
            let cell = self.cache.cell(record, retain)?;
            let store = self.store.clone();
            let record = record.clone();
            if let Some(cell) = cell {
                self.pending.push_back(Box::pin(async move {
                    let raw = cell
                        .get_or_try_init(|| async {
                            read_chunk(store, record.clone())
                                .await
                                .map(|(raw, _)| Arc::new(raw))
                        })
                        .await?;
                    Ok((raw.as_ref().clone(), record.offset))
                }));
            } else {
                self.pending.push_back(Box::pin(read_chunk(store, record)));
            }
            self.pending_bytes = bytes;
            self.next_record = self
                .next_record
                .checked_add(1)
                .ok_or(ServerError::Overflow)?;
        }
        let Some(result) = self.pending.next().await else {
            return Ok(None);
        };
        let (raw, offset) = result?;
        self.pending_bytes = self
            .pending_bytes
            .checked_sub(u64::try_from(raw.len())?)
            .ok_or(ServerError::Overflow)?;
        Ok(Some((raw, offset)))
    }
}

async fn read_chunk(
    store: ServerObjectStore,
    record: FileChunkRecord,
) -> Result<(Vec<u8>, u64), ServerError> {
    let key = crate::chunk_store::chunk_object_key(&record.hash)?;
    let end = record
        .packed_end
        .checked_sub(record.packed_start)
        .and_then(|length| length.checked_sub(1))
        .ok_or(ServerError::Overflow)?;
    let compressed = AsyncObjectStore::read_range(
        &store,
        &key,
        ByteRange::new(0, end).map_err(|_error| ServerError::Overflow)?,
    )
    .await?;
    let raw = decode_and_verify_durable_chunk(&compressed, &record.hash, record.length)?;
    Ok((raw, record.offset))
}

pub(super) fn decode_and_verify_durable_chunk(
    compressed: &[u8],
    expected_hash: &str,
    expected_length: u64,
) -> Result<Vec<u8>, ServerError> {
    // Check before allocating: a corrupt size prefix must not bypass the
    // read-ahead byte budget or allocate far more than the recorded chunk.
    let advertised = compressed
        .first_chunk::<4>()
        .map(|header| u64::from(u32::from_le_bytes(*header)));
    if advertised != Some(expected_length) {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    let raw = lz4_flex::decompress_size_prepended(compressed).map_err(|error| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("stored upload chunk could not be decompressed: {error}"),
        ))
    })?;
    if u64::try_from(raw.len())? != expected_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    if xet_hash_hex_string(crate::local_backend::chunk_hash(&raw)) != expected_hash {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    Ok(raw)
}

#[cfg(test)]
mod tests;
