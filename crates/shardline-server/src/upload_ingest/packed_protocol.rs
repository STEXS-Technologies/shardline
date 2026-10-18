//! Bounded packing for protocol uploads whose public response exposes no raw chunks.
use axum::body::Bytes;
use shardline_index::{FileChunkRecord, xet_hash_hex_string};
use shardline_storage::{ObjectBody, ObjectIntegrity, PutOutcome};

use super::{
    body_reader::ChunkBuffer,
    chunk_store::put_if_absent_pooled_chunk_buffer,
    durable_chunks::{DurableUploadChunks, ReadCache},
    ingestor::MAX_XORB_CHUNKS,
    xorb_packer::RecoverableXorbBatch,
};
use crate::{ServerError, model::UploadChunkResult, object_store::ServerObjectStore};

// The prior fixed 32 MiB batches caused even nearly identical file versions
// to receive different object identities. Cut containers at content-derived
// CDC chunk anchors so shared runs get the same immutable xorb hash. The cap
// bounds damage when an input has no useful anchors.
const MIN_PACKED_XORB_RAW_BYTES: usize = 512 * 1024;
const MAX_PACKED_XORB_RAW_BYTES: usize = 4 * 1024 * 1024;
const XORB_ANCHOR_MASK: u8 = 0x0f;

#[derive(Default)]
pub(super) struct PackedProtocolResult {
    pub(super) records: Vec<FileChunkRecord>,
    pub(super) chunks: Vec<UploadChunkResult>,
    pub(super) inserted: u64,
    pub(super) reused: u64,
    pub(super) stored_bytes: u64,
}

pub(super) struct PackedProtocolUpload {
    store: ServerObjectStore,
    work_pool: crate::admission::BoundedPool,
    pending: Vec<(Vec<u8>, u64)>,
    pending_bytes: usize,
    result: PackedProtocolResult,
    batch_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PackedProtocolUpload {
    pub(super) fn new(store: ServerObjectStore, work_pool: crate::admission::BoundedPool) -> Self {
        Self {
            store,
            work_pool,
            pending: Vec::new(),
            pending_bytes: 0,
            result: PackedProtocolResult::default(),
            batch_permit: None,
        }
    }

    pub(super) async fn push(&mut self, chunk: &[u8], offset: u64) -> Result<(), ServerError> {
        let next = self
            .pending_bytes
            .checked_add(chunk.len())
            .ok_or(ServerError::Overflow)?;
        if !self.pending.is_empty()
            && (next > MAX_PACKED_XORB_RAW_BYTES || self.pending.len() >= MAX_XORB_CHUNKS)
        {
            self.flush().await?;
        }
        if self.batch_permit.is_none() {
            self.batch_permit = Some(
                self.work_pool
                    .acquire()
                    .await
                    .map_err(|error| ServerError::Io(std::io::Error::other(error)))?,
            );
        }
        self.pending_bytes = self
            .pending_bytes
            .checked_add(chunk.len())
            .ok_or(ServerError::Overflow)?;
        self.pending.push((chunk.to_vec(), offset));
        if self.pending_bytes >= MIN_PACKED_XORB_RAW_BYTES && is_xorb_anchor(chunk) {
            self.flush().await?;
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<PackedProtocolResult, ServerError> {
        self.flush().await?;
        Ok(self.result)
    }

    async fn flush(&mut self) -> Result<(), ServerError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let chunks = std::mem::take(&mut self.pending);
        let raw_bytes = u64::try_from(self.pending_bytes)?;
        self.pending_bytes = 0;
        // Keep raw ownership for fallback without cloning a second raw batch.
        // The permit covers raw accumulation, CPU work, read-back and fallback.
        // A cancelled running worker retains it until its buffers are dropped.
        let permit = self.batch_permit.take().ok_or(ServerError::Overflow)?;
        let ((raw, packed), mut _permit) =
            crate::admission::blocking_work_with_permit(permit, move || {
                let raw = RecoverableXorbBatch::new(chunks);
                let packed = raw.pack();
                Ok((raw, packed))
            })
            .await?;
        match packed {
            Ok(mut packed) => {
                let key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex)?;
                let serialized = std::mem::take(&mut packed.serialized);
                let integrity = ObjectIntegrity::new(
                    crate::local_backend::chunk_hash(&serialized),
                    u64::try_from(serialized.len())?,
                );
                // Move the encoded buffer into the request; retaining raw input
                // must not require another whole-container payload copy.
                let stored = match &self.store {
                    ServerObjectStore::S3(store) => store
                        .put_if_absent_with_read_back(
                            &key,
                            ObjectBody::from_bytes(Bytes::from(serialized)),
                            &integrity,
                        )
                        .await
                        .map_err(ServerError::from),
                    ServerObjectStore::Local(store) => {
                        let store = store.clone();
                        let (result, retained) =
                            crate::admission::blocking_work_with_permit(_permit, move || {
                                Ok(local_store::store_verified(
                                    &store,
                                    &key,
                                    &serialized,
                                    &integrity,
                                ))
                            })
                            .await?;
                        _permit = retained;
                        result
                    }
                    ServerObjectStore::Blackhole => return Err(ServerError::Overflow),
                };
                match stored {
                    Ok(outcome) => {
                        let inserted = outcome == PutOutcome::Inserted;
                        let recovered_chunks = raw.into_chunks();
                        if recovered_chunks.len() != packed.chunk_entries.len() {
                            return Err(ServerError::Overflow);
                        }
                        crate::metrics::record_compression_saved(raw_bytes, integrity.length());
                        for ((bytes, offset), entry) in
                            recovered_chunks.into_iter().zip(packed.chunk_entries)
                        {
                            let length = u64::try_from(bytes.len())?;
                            if offset != entry.raw_offset || length != entry.raw_length {
                                return Err(ServerError::Overflow);
                            }
                            self.record_result(&bytes, offset, inserted)?;
                            self.result.records.push(FileChunkRecord {
                                hash: packed.xorb_hash_hex.clone(),
                                offset,
                                length,
                                range_start: u64::from(entry.chunk_index),
                                range_end: u64::from(
                                    entry
                                        .chunk_index
                                        .checked_add(1)
                                        .ok_or(ServerError::Overflow)?,
                                ),
                                packed_start: u64::from(entry.packed_offset),
                                packed_end: u64::from(
                                    entry
                                        .packed_offset
                                        .checked_add(entry.packed_length)
                                        .ok_or(ServerError::Overflow)?,
                                ),
                            });
                        }
                        return Ok(());
                    }
                    Err(error) => {
                        tracing::warn!(%error, "packed protocol storage failed; using verified individual chunks")
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "protocol packing failed; using verified individual chunks")
            }
        }
        self.store_individual_fallback(raw.into_chunks()).await
    }

    async fn store_individual_fallback(
        &mut self,
        chunks: Vec<(Vec<u8>, u64)>,
    ) -> Result<(), ServerError> {
        let objects = self.store.clone();
        for (bytes, offset) in chunks {
            let (outcome, _) = put_if_absent_pooled_chunk_buffer(
                &objects,
                ChunkBuffer::Pooled(Bytes::copy_from_slice(&bytes)),
            )
            .await?;
            let record = FileChunkRecord {
                hash: outcome.hash_hex,
                offset,
                length: outcome.chunk_length,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: outcome.compressed_length,
            };
            if outcome.inserted {
                let mut reader =
                    DurableUploadChunks::with_cache(objects.clone(), 1, ReadCache::default());
                reader
                    .next(std::slice::from_ref(&record))
                    .await?
                    .ok_or(ServerError::Overflow)?;
            }
            self.record_result(&bytes, offset, outcome.inserted)?;
            self.result.records.push(record);
        }
        Ok(())
    }

    fn record_result(
        &mut self,
        bytes: &[u8],
        offset: u64,
        inserted: bool,
    ) -> Result<(), ServerError> {
        let length = u64::try_from(bytes.len())?;
        if inserted {
            self.result.inserted = self
                .result
                .inserted
                .checked_add(1)
                .ok_or(ServerError::Overflow)?;
            self.result.stored_bytes = self
                .result
                .stored_bytes
                .checked_add(length)
                .ok_or(ServerError::Overflow)?;
            crate::metrics::record_chunk_inserted(length);
        } else {
            self.result.reused = self
                .result
                .reused
                .checked_add(1)
                .ok_or(ServerError::Overflow)?;
            crate::metrics::record_dedup_saves(length);
        }
        self.result.chunks.push(UploadChunkResult {
            hash: xet_hash_hex_string(crate::local_backend::chunk_hash(bytes)),
            offset,
            length,
            inserted,
        });
        Ok(())
    }
}

fn is_xorb_anchor(chunk: &[u8]) -> bool {
    blake3::hash(chunk).as_bytes()[0] & XORB_ANCHOR_MASK == 0
}

mod local_store;

#[cfg(test)]
mod tests;
