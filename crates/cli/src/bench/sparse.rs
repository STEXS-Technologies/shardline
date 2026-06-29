use std::iter::repeat_n;

use bytes::Bytes;
use shardline_protocol::ByteRange;

use super::{BenchRuntimeError, ConcurrentIngestUploadCase, ConcurrentUploadCase};

pub(crate) fn build_concurrent_upload_cases(
    base: &[u8],
    mutated_bytes: usize,
    chunk_size: usize,
    concurrency: u32,
) -> Result<Vec<ConcurrentUploadCase>, BenchRuntimeError> {
    let chunk_count = base.len().div_ceil(chunk_size);
    let mut cases = Vec::with_capacity(usize::try_from(concurrency)?);
    for worker_index in 0..concurrency {
        let worker_index_usize = usize::try_from(worker_index)?;
        let selected_chunk = worker_index_usize
            .checked_rem(chunk_count)
            .ok_or(BenchRuntimeError::ConcurrentUploadChunkSelectionFailed)?;
        let chunk_start = selected_chunk
            .checked_mul(chunk_size)
            .ok_or(BenchRuntimeError::ChunkStartOverflow)?;
        let chunk_end = chunk_start
            .checked_add(chunk_size)
            .map(|value| value.min(base.len()))
            .ok_or(BenchRuntimeError::ChunkEndOverflow)?;
        let window_length = mutated_bytes.min(
            chunk_end
                .checked_sub(chunk_start)
                .ok_or(BenchRuntimeError::ChunkWindowUnderflow)?,
        );
        let expected_bytes = build_worker_update(base, chunk_start, window_length, worker_index)?;
        cases.push(ConcurrentUploadCase {
            file_id: format!("concurrent-{worker_index:04}.bin"),
            expected_bytes,
        });
    }

    Ok(cases)
}

pub(crate) fn build_concurrent_ingest_upload_cases(
    base: &[u8],
    mutated_bytes: usize,
    chunk_size: usize,
    concurrency: u32,
) -> Result<Vec<ConcurrentIngestUploadCase>, BenchRuntimeError> {
    let upload_cases = build_concurrent_upload_cases(base, mutated_bytes, chunk_size, concurrency)?;
    let cases = upload_cases
        .into_iter()
        .map(|case| ConcurrentIngestUploadCase {
            file_id: case.file_id,
            body: case.expected_bytes,
        })
        .collect();
    Ok(cases)
}

pub(crate) fn build_worker_update(
    base: &[u8],
    start: usize,
    mutated_bytes: usize,
    worker_index: u32,
) -> Result<Bytes, BenchRuntimeError> {
    let mut updated = base.to_vec();
    let end = start
        .checked_add(mutated_bytes)
        .ok_or_else(|| BenchRuntimeError::WorkerMutationWindowOverflow)?;
    let window = updated
        .get_mut(start..end)
        .ok_or_else(|| BenchRuntimeError::WorkerMutationWindowOutOfBounds)?;
    let worker_seed = usize::try_from(worker_index)?;
    for (offset, byte) in window.iter_mut().enumerate() {
        let delta_source = worker_seed
            .checked_mul(17)
            .and_then(|value| value.checked_add(offset.saturating_mul(13)))
            .and_then(|value| value.checked_add(31))
            .ok_or(BenchRuntimeError::WorkerDeltaOverflow)?;
        let delta = u8::try_from(delta_source % 251)?;
        *byte = byte.wrapping_add(delta).wrapping_add(1);
    }

    Ok(Bytes::from(updated))
}

pub(crate) fn build_base_asset(length: usize) -> Result<Vec<u8>, BenchRuntimeError> {
    let mut bytes = Vec::with_capacity(length);
    for index in 0..length {
        let value = u8::try_from((index.saturating_mul(31).saturating_add(17)) % 251)?;
        bytes.push(value);
    }

    Ok(bytes)
}

pub(crate) fn build_sparse_update(
    base: &[u8],
    mutated_bytes: usize,
) -> Result<Vec<u8>, BenchRuntimeError> {
    let mut updated = base.to_vec();
    let remaining = base
        .len()
        .checked_sub(mutated_bytes)
        .ok_or(BenchRuntimeError::MutatedBytesExceedBaseBytes)?;
    let start = remaining
        .checked_div(2)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)?;
    let end = start
        .checked_add(mutated_bytes)
        .ok_or(BenchRuntimeError::MutationWindowOverflow)?;
    let window = updated
        .get_mut(start..end)
        .ok_or(BenchRuntimeError::MutationWindowOutOfBounds)?;
    for (offset, byte) in window.iter_mut().enumerate() {
        let delta = u8::try_from((offset.saturating_mul(13).saturating_add(29)) % 251)?;
        *byte = byte.wrapping_add(delta).wrapping_add(1);
    }

    Ok(updated)
}

pub(crate) fn build_mutation_range(
    base_bytes: usize,
    mutated_bytes: usize,
) -> Result<ByteRange, BenchRuntimeError> {
    let remaining = base_bytes
        .checked_sub(mutated_bytes)
        .ok_or(BenchRuntimeError::MutatedBytesExceedBaseBytes)?;
    let start = remaining
        .checked_div(2)
        .ok_or(BenchRuntimeError::BenchmarkDivisorZero)?;
    let end = start
        .checked_add(mutated_bytes)
        .and_then(|value| value.checked_sub(1))
        .ok_or(BenchRuntimeError::MutationRangeOverflow)?;
    let start = u64::try_from(start)?;
    let end = u64::try_from(end)?;

    ByteRange::new(start, end).map_err(BenchRuntimeError::MutationRangeInvalid)
}

pub(crate) fn build_cross_repository_assets(
    chunk_size: usize,
) -> Result<(Vec<u8>, Vec<u8>), BenchRuntimeError> {
    let capacity = chunk_size
        .checked_mul(3)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryAssetOverflow)?;
    let mut base = Vec::with_capacity(capacity);
    base.extend(repeat_n(0x11, chunk_size));
    base.extend(repeat_n(0x22, chunk_size));
    base.extend(repeat_n(0x33, chunk_size));

    let mut updated = base.clone();
    let middle_start = chunk_size;
    let middle_end = chunk_size
        .checked_mul(2)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryAssetOverflow)?;
    let middle = updated
        .get_mut(middle_start..middle_end)
        .ok_or_else(|| BenchRuntimeError::CrossRepositoryMiddleChunkOutOfBounds)?;
    middle.fill(0x44);

    Ok((base, updated))
}
