//! Authenticate selected decoded members against the independently trusted Xorb root.
//! This proves returned bytes, not the health of unrequested payloads. Full-file
//! downloads and unsupported legacy footers retain complete-container validation.
use super::{Bytes, ServerByteStream, ServerError, ServerObjectStore};
use crate::admission::{BoundedPool, blocking_work_with_permit};
use futures_util::{StreamExt, stream};
use shardline_index::{FileRecord, StorageRepresentation, parse_xet_hash_hex};
use shardline_protocol::ByteRange;
use shardline_storage::S3RangeSnapshot;
use shardline_xet_adapter::{
    AuthenticatedXorbFooter, decode_authenticated_xorb_chunk, validate_serialized_xorb_footer,
};
use std::{io, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

const MAX_FOOTER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SERIALIZED_RANGE_BYTES: u64 = 1024 * 1024;

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
    snapshot: S3RangeSnapshot,
    footer: AuthenticatedXorbFooter,
    selections: std::vec::IntoIter<Selection>,
    pool: BoundedPool,
    // Retained by metadata, blocking jobs and transport-owned response slices.
    memory: Arc<OwnedSemaphorePermit>,
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
            .len()
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
    let memory = Arc::new(
        pool.acquire_buffer()
            .await
            .map_err(|error| ServerError::Io(io::Error::other(error)))?,
    );
    let key = crate::xet_adapter::xorb_object_key(&first.hash)?;
    let ServerObjectStore::S3(s3) = &store else {
        return Ok(None);
    };
    let Some((trailer, snapshot)) = s3.read_suffix_snapshot(&key, 8).await? else {
        return Ok(None);
    };
    let total = snapshot.length();
    let expected = parse_xet_hash_hex(&first.hash)?;
    let (_, footer, _) =
        load_footer(&snapshot, &trailer, total, expected, &pool, memory.clone()).await?;
    let Some(footer) = footer else {
        return Ok(None);
    };
    let record = record.clone();
    let worker = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(io::Error::other(error)))?;
    let ((footer, selections), _) =
        blocking_work_with_permit((memory.clone(), worker), move || {
            let selections = select(&record, range, &footer)?;
            Ok((footer, selections))
        })
        .await?;
    let state = State {
        snapshot,
        footer,
        selections: selections.into_iter(),
        pool,
        memory,
    };
    let mut output = Box::pin(stream::try_unfold(state, next));
    // Check the first selected payload before callers send success headers.
    let first_bytes = output.next().await.ok_or_else(invalid)??;
    Ok(Some(Box::pin(
        stream::once(async move { Ok(first_bytes) }).chain(output),
    )))
}

/// Serves a bounded serialized-byte slice from authenticated member encodings.
/// The complete intersecting members are fetched and decoded before their original
/// packed-byte slices are returned. Unsupported footer versions use strict fallback;
/// malformed proofs and member corruption remain errors.
pub(crate) async fn serialized_stream(
    store: &ServerObjectStore,
    hash: &str,
    range: ByteRange,
    coverage: Option<&crate::record_store::XorbRangeCoverage>,
    pool: BoundedPool,
) -> Result<Option<(u64, ServerByteStream)>, ServerError> {
    let ServerObjectStore::S3(s3) = store else {
        return Ok(None);
    };
    if range
        .len()
        .is_none_or(|length| length > MAX_SERIALIZED_RANGE_BYTES)
    {
        return Ok(None);
    }
    // Establish that the exact requested serialized span belongs to the scoped
    // repository candidate before any provider request. Footer reads for owners
    // requiring the whole-payload exception remain on the strict path.
    if coverage.is_some_and(|allowed| !allowed.covers(range)) {
        return Ok(None);
    }
    let memory = Arc::new(
        pool.acquire_buffer()
            .await
            .map_err(|error| ServerError::Io(io::Error::other(error)))?,
    );
    let key = crate::xet_adapter::xorb_object_key(hash)?;
    let Some((trailer, snapshot)) = s3.read_suffix_snapshot(&key, 8).await? else {
        return Err(crate::object_store::ServerObjectStoreError::NotFound.into());
    };
    let total = snapshot.length();
    if range.end_inclusive() >= total {
        return Err(ServerError::RangeNotSatisfiable);
    }
    let expected = parse_xet_hash_hex(hash)?;
    let (footer_start, footer, footer_bytes) =
        load_footer(&snapshot, &trailer, total, expected, &pool, memory.clone()).await?;
    let Some(footer) = footer else {
        return Ok(None);
    };
    let footer = Arc::new(footer);
    let packed_length = footer.chunks().last().ok_or_else(invalid)?.packed_end();
    let layout_coverage = coverage.map(|allowed| allowed.for_layout(footer.chunks()));
    if layout_coverage
        .as_ref()
        .is_some_and(|allowed| !allowed.permits(range, packed_length))
    {
        return Err(ServerError::NotFound);
    }
    let end = range.end_inclusive().checked_add(1).ok_or_else(invalid)?;
    let mut output = Vec::with_capacity(usize::try_from(range.len().ok_or_else(invalid)?)?);
    if range.start() < footer_start {
        let packed_end = end.min(footer_start);
        let mut selected = Vec::new();
        for (index, member) in footer.chunks().iter().enumerate() {
            let overlap_start = range.start().max(member.packed_start());
            let overlap_end = packed_end.min(member.packed_end());
            if overlap_start >= overlap_end {
                continue;
            }
            if selected.len() >= 4096 {
                return Ok(None);
            }
            selected.push(Selection {
                index,
                start: usize::try_from(
                    overlap_start
                        .checked_sub(member.packed_start())
                        .ok_or_else(invalid)?,
                )?,
                end: usize::try_from(
                    overlap_end
                        .checked_sub(member.packed_start())
                        .ok_or_else(invalid)?,
                )?,
            });
        }
        let packed_total = selected.iter().try_fold(0_u64, |sum, selection| {
            let member = footer.chunks().get(selection.index).ok_or(())?;
            member
                .packed_end()
                .checked_sub(member.packed_start())
                .and_then(|length| sum.checked_add(length))
                .ok_or(())
        });
        let decoded_total = selected.iter().try_fold(0_u64, |sum, selection| {
            let member = footer.chunks().get(selection.index).ok_or(())?;
            sum.checked_add(member.unpacked_len()).ok_or(())
        });
        if packed_total.map_or(true, |sum| sum > 8 * 1024 * 1024)
            || decoded_total.map_or(true, |sum| sum > 8 * 1024 * 1024)
        {
            return Ok(None);
        }
        for selection in selected {
            let member = footer.chunks().get(selection.index).ok_or_else(invalid)?;
            let member_start = member.packed_start();
            let member_end = member.packed_end();
            let packed = snapshot
                .read_range(
                    ByteRange::new(member_start, member_end.checked_sub(1).ok_or_else(invalid)?)
                        .map_err(|_range| invalid())?,
                )
                .await?;
            let worker = pool
                .acquire()
                .await
                .map_err(|error| ServerError::Io(io::Error::other(error)))?;
            let footer_for_worker = footer.clone();
            let (packed, _) = blocking_work_with_permit((memory.clone(), worker), move || {
                decode_authenticated_xorb_chunk(&footer_for_worker, selection.index, &packed)?;
                Ok(packed)
            })
            .await?;
            // The authenticated packed bytes are emitted verbatim; no re-encoding occurs.
            output.extend_from_slice(
                packed
                    .get(selection.start..selection.end)
                    .ok_or_else(invalid)?,
            );
        }
    }
    if end > footer_start {
        let footer_slice_start = range.start().max(footer_start);
        let from = usize::try_from(
            footer_slice_start
                .checked_sub(footer_start)
                .ok_or_else(invalid)?,
        )?;
        let to = usize::try_from(end.checked_sub(footer_start).ok_or_else(invalid)?)?;
        output.extend_from_slice(footer_bytes.get(from..to).ok_or_else(invalid)?);
    }
    if output.len() != usize::try_from(range.len().ok_or_else(invalid)?)? {
        return Err(invalid());
    }
    let body = crate::admission::retain_bytes(output, memory);
    let stream = Box::pin(stream::once(async move { Ok(body) }));
    Ok(Some((total, stream)))
}

async fn load_footer(
    snapshot: &S3RangeSnapshot,
    trailer: &[u8],
    total: u64,
    expected: shardline_protocol::ShardlineHash,
    pool: &BoundedPool,
    memory: Arc<OwnedSemaphorePermit>,
) -> Result<(u64, Option<AuthenticatedXorbFooter>, Vec<u8>), ServerError> {
    let trailer: [u8; 8] = trailer.try_into().map_err(|_length| invalid())?;
    let footer_len = u64::from_le_bytes(trailer)
        .checked_add(8)
        .ok_or_else(invalid)?;
    if !(8..=MAX_FOOTER_BYTES).contains(&footer_len) || footer_len > total {
        return Err(invalid());
    }
    let footer_start = total.checked_sub(footer_len).ok_or_else(invalid)?;
    let footer_bytes = snapshot
        .read_range(
            ByteRange::new(footer_start, total.checked_sub(1).ok_or_else(invalid)?)
                .map_err(|_range| invalid())?,
        )
        .await?;
    let worker = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(io::Error::other(error)))?;
    let (validated, _) = blocking_work_with_permit((memory, worker), move || {
        let footer = validate_serialized_xorb_footer(&footer_bytes, total, expected)?;
        Ok((footer, footer_bytes))
    })
    .await?;
    let (footer, validated_bytes) = validated;
    Ok((footer_start, footer, validated_bytes))
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
    let packed = state.snapshot.read_range(range).await?;
    let footer = state.footer;
    let worker = state
        .pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(io::Error::other(error)))?;
    let memory = state.memory.clone();
    let ((footer, bytes), _) = blocking_work_with_permit((memory.clone(), worker), move || {
        let decoded = decode_authenticated_xorb_chunk(&footer, selection.index, &packed)?;
        let selected = decoded
            .data()
            .get(selection.start..selection.end)
            .ok_or_else(invalid)?;
        Ok((
            footer,
            crate::admission::retain_bytes(selected.to_vec(), memory),
        ))
    })
    .await?;
    state.footer = footer;
    Ok(Some((bytes, state)))
}
