//! Authenticate selected decoded members against the independently trusted Xorb root.
//! This proves returned bytes, not the health of unrequested payloads. Full-file
//! downloads and unsupported legacy footers retain complete-container validation.
use super::{Bytes, ServerByteStream, ServerError, ServerObjectStore};
use crate::admission::{BoundedPool, blocking_work_with_permit};
use futures_util::{StreamExt, stream};
use shardline_index::{FileRecord, StorageRepresentation, parse_xet_hash_hex};
use shardline_protocol::ByteRange;
use shardline_storage::{AsyncObjectStore, ObjectKey};
use shardline_xet_adapter::{
    AuthenticatedXorbFooter, decode_authenticated_xorb_chunk, validate_serialized_xorb_footer,
};
use std::io;
use tokio::sync::OwnedSemaphorePermit;

const MAX_FOOTER_BYTES: u64 = 16 * 1024 * 1024;

fn invalid() -> ServerError {
    ServerError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid authenticated Xorb range",
    ))
}

struct Selection {
    index: usize,
    start: usize,
    end: usize,
}

struct State {
    store: ServerObjectStore,
    key: ObjectKey,
    footer: AuthenticatedXorbFooter,
    selections: std::vec::IntoIter<Selection>,
    // Retained through storage awaits, blocking decode and slow-reader backpressure.
    permit: OwnedSemaphorePermit,
}

pub(super) async fn file_stream(
    store: ServerObjectStore,
    record: &FileRecord,
    range: ByteRange,
    pool: BoundedPool,
) -> Result<Option<ServerByteStream>, ServerError> {
    // Small containers and broad ranges favor the existing single GET. Legacy
    // compressed standalone chunks have only 0..1 terms, not Xorb member plans.
    if !matches!(store, ServerObjectStore::S3(_))
        || record.storage_repr != StorageRepresentation::XorbCdcV1
        || record.total_bytes <= 1024 * 1024
        || range.end_inclusive() >= record.total_bytes
        || range
            .end_inclusive()
            .checked_sub(range.start())
            .and_then(|n| n.checked_add(1))
            .is_none_or(|length| length >= record.total_bytes / 2)
        || !record
            .chunks
            .iter()
            .any(|term| term.range_start > 0 || term.range_end > 1)
    {
        return Ok(None);
    }
    let first = record.chunks.first().ok_or_else(invalid)?;
    if !record
        .chunks
        .iter()
        .all(|term| term.hash == first.hash && term.packed_end > 0)
    {
        return Ok(None);
    }
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(io::Error::other(error)))?;
    let key = crate::xet_adapter::xorb_object_key(&first.hash)?;
    let Some((trailer, total)) = AsyncObjectStore::read_suffix(&store, &key, 8).await? else {
        return Ok(None);
    };
    let trailer: [u8; 8] = trailer.try_into().map_err(|_length| invalid())?;
    let length = u64::from_le_bytes(trailer)
        .checked_add(8)
        .ok_or_else(invalid)?;
    if !(8..=MAX_FOOTER_BYTES).contains(&length) || length > total {
        return Err(invalid());
    }
    let bytes = AsyncObjectStore::read_range(
        &store,
        &key,
        ByteRange::new(
            total.checked_sub(length).ok_or_else(invalid)?,
            total.checked_sub(1).ok_or_else(invalid)?,
        )
        .map_err(|_range| invalid())?,
    )
    .await?;
    let expected = parse_xet_hash_hex(&first.hash)?;
    let record = record.clone();
    let (authenticated, permit) = blocking_work_with_permit(permit, move || {
        let Some(footer) = validate_serialized_xorb_footer(&bytes, total, expected)? else {
            return Ok(None);
        };
        let selections = select(&record, range, &footer)?;
        Ok(Some((footer, selections)))
    })
    .await?;
    let Some((footer, selections)) = authenticated else {
        return Ok(None);
    };
    let state = State {
        store,
        key,
        footer,
        selections: selections.into_iter(),
        permit,
    };
    let mut output = Box::pin(stream::try_unfold(state, next));
    // Check the first selected payload before callers send success headers.
    let first_bytes = output.next().await.ok_or_else(invalid)??;
    Ok(Some(Box::pin(
        stream::once(async move { Ok(first_bytes) }).chain(output),
    )))
}

fn select(
    record: &FileRecord,
    range: ByteRange,
    footer: &AuthenticatedXorbFooter,
) -> Result<Vec<Selection>, ServerError> {
    let mut selections = Vec::new();
    for term in &record.chunks {
        let first = usize::try_from(term.range_start)?;
        let limit = usize::try_from(term.range_end)?;
        let members = footer.chunks().get(first..limit).ok_or_else(invalid)?;
        let begin = members.first().ok_or_else(invalid)?;
        let last = members.last().ok_or_else(invalid)?;
        if last.unpacked_end().checked_sub(begin.unpacked_start()) != Some(term.length)
            || term.packed_start != begin.packed_start()
            || term.packed_end != last.packed_end()
        {
            return Err(invalid());
        }
        for (offset, member) in members.iter().enumerate() {
            let logical = term
                .offset
                .checked_add(
                    member
                        .unpacked_start()
                        .checked_sub(begin.unpacked_start())
                        .ok_or_else(invalid)?,
                )
                .ok_or_else(invalid)?;
            let end = logical
                .checked_add(member.unpacked_len())
                .ok_or_else(invalid)?;
            let start = range.start().max(logical);
            let stop = range
                .end_inclusive()
                .checked_add(1)
                .ok_or_else(invalid)?
                .min(end);
            if start < stop {
                selections.push(Selection {
                    index: first.checked_add(offset).ok_or_else(invalid)?,
                    start: usize::try_from(start.checked_sub(logical).ok_or_else(invalid)?)?,
                    end: usize::try_from(stop.checked_sub(logical).ok_or_else(invalid)?)?,
                });
            }
        }
    }
    Ok(selections)
}

async fn next(mut state: State) -> Result<Option<(Bytes, State)>, ServerError> {
    let Some(selection) = state.selections.next() else {
        return Ok(None);
    };
    let descriptor = state
        .footer
        .chunks()
        .get(selection.index)
        .ok_or_else(invalid)?;
    let range = ByteRange::new(
        descriptor.packed_start(),
        descriptor.packed_end().checked_sub(1).ok_or_else(invalid)?,
    )
    .map_err(|_range| invalid())?;
    let packed = AsyncObjectStore::read_range(&state.store, &state.key, range).await?;
    let footer = state.footer;
    let ((footer, bytes), permit) = blocking_work_with_permit(state.permit, move || {
        let decoded = decode_authenticated_xorb_chunk(&footer, selection.index, &packed)?;
        let selected = decoded
            .data()
            .get(selection.start..selection.end)
            .ok_or_else(invalid)?;
        Ok((footer, Bytes::copy_from_slice(selected)))
    })
    .await?;
    state.footer = footer;
    state.permit = permit;
    Ok(Some((bytes, state)))
}
