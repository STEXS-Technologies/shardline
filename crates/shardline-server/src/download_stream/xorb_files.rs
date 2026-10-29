//! Route logical file terms independently of the number of packed containers.
//!
//! Only one container stream is alive at a time. Logical file offsets are
//! rebased for each group; chunk-index ranges remain relative to the container.
use futures_util::{StreamExt, TryStreamExt, stream};
use shardline_index::{FileRecord, StorageRepresentation};
use shardline_protocol::ByteRange;

use super::{ServerByteStream, single_container_file_stream};
use crate::{ServerError, admission::BoundedPool, object_store::ServerObjectStore};

pub(super) async fn file_stream(
    object_store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
) -> Result<ServerByteStream, ServerError> {
    file_stream_with_decode_budget(object_store, record, range, pool, 0).await
}

pub(super) async fn file_stream_with_decode_budget(
    object_store: ServerObjectStore,
    mut record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
    decoded_byte_budget: usize,
) -> Result<ServerByteStream, ServerError> {
    record.validate_reconstruction_plan()?;
    if record.total_bytes == 0 || record.storage_repr == StorageRepresentation::WholeFileV1 {
        return single_container_file_stream(
            object_store,
            record,
            range,
            pool,
            decoded_byte_budget,
        )
        .await;
    }
    let requested_start = range.map_or(0, |value| value.start());
    let requested_end = range.map_or(
        record
            .total_bytes
            .checked_sub(1)
            .ok_or(ServerError::Overflow)?,
        |value| value.end_inclusive(),
    );
    if requested_end >= record.total_bytes {
        return Err(ServerError::RangeNotSatisfiable);
    }

    let mut chunks = std::mem::take(&mut record.chunks).into_iter().peekable();
    let mut groups = Vec::new();
    while let Some(mut chunk) = chunks.next() {
        let base = chunk.offset;
        let mut group = record.clone();
        group.total_bytes = chunk.length;
        chunk.offset = 0;
        let hash = chunk.hash.clone();
        group.chunks.push(chunk);
        while chunks.peek().is_some_and(|next| next.hash == hash) {
            let mut next_chunk = chunks.next().ok_or(ServerError::Overflow)?;
            next_chunk.offset = next_chunk
                .offset
                .checked_sub(base)
                .ok_or(ServerError::Overflow)?;
            group.total_bytes = group
                .total_bytes
                .checked_add(next_chunk.length)
                .ok_or(ServerError::Overflow)?;
            group.chunks.push(next_chunk);
        }
        let end = base
            .checked_add(group.total_bytes)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        let start = requested_start.max(base);
        let end = requested_end.min(end);
        if start <= end {
            let relative = ByteRange::new(
                start.checked_sub(base).ok_or(ServerError::Overflow)?,
                end.checked_sub(base).ok_or(ServerError::Overflow)?,
            )
            .map_err(|_error| ServerError::RangeNotSatisfiable)?;
            groups.push((group, Some(relative)));
        }
    }

    let mut groups = groups.into_iter();
    let (first, first_range) = groups.next().ok_or(ServerError::Overflow)?;
    // Validate the first payload before exposing the stream. Explicit ranges
    // can authenticate selected members; full downloads verify each container.
    // Later groups retain neither earlier scratch files nor a whole logical file.
    let first = container_stream(
        object_store.clone(),
        first,
        first_range,
        pool.clone(),
        decoded_byte_budget,
        range.is_some(),
    )
    .await?;
    let remaining = stream::iter(groups).then(move |(next_record, next_range)| {
        let store = object_store.clone();
        let pool = pool.clone();
        async move {
            container_stream(
                store,
                next_record,
                next_range,
                pool,
                decoded_byte_budget,
                range.is_some(),
            )
            .await
        }
    });
    Ok(Box::pin(
        stream::once(async move { Ok(first) })
            .chain(remaining)
            .try_flatten(),
    ))
}

async fn container_stream(
    store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
    decoded_byte_budget: usize,
    explicit_range: bool,
) -> Result<ServerByteStream, ServerError> {
    if explicit_range
        && let Some(range) = range
        && let Some(stream) =
            super::authenticated_range::file_stream(store.clone(), &record, range, pool.clone())
                .await?
    {
        return Ok(stream);
    }
    single_container_file_stream(store, record, range, pool, decoded_byte_budget).await
}

#[cfg(test)]
mod tests;
