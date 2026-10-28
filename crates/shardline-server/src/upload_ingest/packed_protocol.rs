//! Shared bounded container packing, with optional standalone-chunk accounting.
use shardline_index::{FileChunkRecord, xet_hash_hex_string};
use shardline_storage::{ObjectBody, ObjectIntegrity, PutOutcome};

use super::{
    body_reader::ChunkBuffer,
    chunk_store::put_if_absent_pooled_chunk_buffer,
    durable_chunks::{DurableUploadChunks, ReadCache},
    ingestor::{MAX_SERVER_XORB_RAW_BYTES, MAX_XORB_CHUNKS},
    xorb_packer::RecoverableXorbBatch,
};
use crate::{ServerError, model::UploadChunkResult, object_store::ServerObjectStore};

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
    report_chunks: bool,
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
            report_chunks: true,
        }
    }

    /// Public upload accounting belongs to the independently persisted chunks.
    /// Reuse the same bounded packer without reporting those chunks twice.
    pub(super) fn for_individual_chunks(
        store: ServerObjectStore,
        work_pool: crate::admission::BoundedPool,
    ) -> Self {
        Self {
            report_chunks: false,
            ..Self::new(store, work_pool)
        }
    }

    pub(super) async fn push(&mut self, chunk: &[u8], offset: u64) -> Result<(), ServerError> {
        let next = self
            .pending_bytes
            .checked_add(chunk.len())
            .ok_or(ServerError::Overflow)?;
        if !self.pending.is_empty()
            && (next > MAX_SERVER_XORB_RAW_BYTES || self.pending.len() >= MAX_XORB_CHUNKS)
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
        let ((raw, packed), permit) =
            crate::admission::blocking_work_with_permit(permit, move || {
                let raw = RecoverableXorbBatch::new(chunks);
                let packed = raw.pack();
                Ok((raw, packed))
            })
            .await?;
        // The encoded payload may outlive a cancelled request in transport.
        // Share the same charge with its owner and with fallback/CPU work.
        let mut _permit = std::sync::Arc::new(permit);
        match packed {
            Ok(mut packed) => {
                let integrity = ObjectIntegrity::new(
                    crate::local_backend::chunk_hash(&packed.serialized),
                    u64::try_from(packed.serialized.len())?,
                );
                let key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex)?;
                let serialized = std::mem::take(&mut packed.serialized);
                // Move the encoded buffer into the request; retaining raw input
                // must not require another whole-container payload copy.
                let mut stored = match &self.store {
                    ServerObjectStore::S3(store) => store
                        .put_if_absent_with_read_back(
                            &key,
                            ObjectBody::from_bytes(super::buffer_admission::into_bytes(
                                serialized,
                                _permit.clone(),
                            )),
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
                let mut retained_length = integrity.length();
                if stored.as_ref().is_err_and(encoding::is_encoding_conflict) {
                    let store = self.store.clone();
                    let ((retained_packed, reused), retained_permit) =
                        crate::admission::blocking_work_with_permit(_permit, move || {
                            let reused = encoding::reuse_retained(&store, &mut packed);
                            Ok((packed, reused))
                        })
                        .await?;
                    _permit = retained_permit;
                    packed = retained_packed;
                    if let Ok(Some(length)) = reused {
                        retained_length = length;
                        stored = Ok(PutOutcome::AlreadyExists);
                    }
                }
                match stored {
                    Ok(outcome) => {
                        let inserted = outcome == PutOutcome::Inserted;
                        let recovered_chunks = raw.into_chunks();
                        if recovered_chunks.len() != packed.chunk_entries.len() {
                            return Err(ServerError::Overflow);
                        }
                        if self.report_chunks {
                            crate::metrics::record_compression_saved(raw_bytes, retained_length);
                        }
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
        self.store_individual_fallback(raw.into_chunks(), _permit)
            .await
    }

    async fn store_individual_fallback(
        &mut self,
        chunks: Vec<(Vec<u8>, u64)>,
        permit: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), ServerError> {
        let objects = self.store.clone();
        for (bytes, offset) in chunks {
            // The batch already owns this allocation. Share it with chunk
            // persistence and accounting instead of copying the raw member.
            let bytes = super::buffer_admission::into_bytes(bytes, permit.clone());
            let (outcome, _) =
                put_if_absent_pooled_chunk_buffer(&objects, ChunkBuffer(bytes.clone())).await?;
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
        if !self.report_chunks {
            return Ok(());
        }
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

mod encoding;
mod local_store;

#[cfg(test)]
mod tests;
