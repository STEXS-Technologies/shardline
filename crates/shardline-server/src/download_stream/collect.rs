//! Reuse bounded validation output only for callers already requesting a Vec.
use futures_util::StreamExt;
use shardline_index::FileRecord;
use shardline_xet_adapter::ValidatedXorb;

use crate::{ServerError, admission::BoundedPool, object_store::ServerObjectStore};

const MAX_COLLECTED_XORB_BYTES: u64 = 1024 * 1024;

pub(crate) async fn collect_file_record_bytes(
    store: ServerObjectStore,
    record: FileRecord,
    pool: BoundedPool,
) -> Result<Vec<u8>, ServerError> {
    let expected = usize::try_from(record.total_bytes)?;
    let budget = if record.total_bytes <= MAX_COLLECTED_XORB_BYTES {
        expected
    } else {
        0
    };
    let mut stream =
        super::xorb_files::file_stream_with_decode_budget(store, record, None, pool, budget)
            .await?;
    let Some(first) = stream.next().await.transpose()? else {
        return if expected == 0 {
            Ok(Vec::new())
        } else {
            Err(length_mismatch())
        };
    };
    let Some(second) = stream.next().await.transpose()? else {
        if first.len() != expected {
            return Err(length_mismatch());
        }
        // Bytes owns the validator's Vec exclusively on the complete-xorb path;
        // conversion transfers its allocation rather than allocating a copy.
        return Ok(Vec::from(first));
    };
    let mut output = Vec::with_capacity(expected);
    for bytes in [first, second] {
        append(&mut output, &bytes, expected)?;
    }
    while let Some(bytes) = stream.next().await {
        append(&mut output, &bytes?, expected)?;
    }
    if output.len() != expected {
        return Err(length_mismatch());
    }
    Ok(output)
}

fn append(output: &mut Vec<u8>, bytes: &[u8], expected: usize) -> Result<(), ServerError> {
    if bytes.len() > expected.saturating_sub(output.len()) {
        return Err(length_mismatch());
    }
    output.extend_from_slice(bytes);
    Ok(())
}

const fn length_mismatch() -> ServerError {
    ServerError::ObjectStore(crate::error::ObjectStoreError::StoredLengthMismatch)
}

pub(super) fn covers_complete_xorb(
    record: &FileRecord,
    validated: &ValidatedXorb,
) -> Result<bool, ServerError> {
    if record.total_bytes != validated.unpacked_length() {
        return Ok(false);
    }
    let mut next = 0;
    for term in &record.chunks {
        let first = usize::try_from(term.range_start)?;
        let limit = usize::try_from(term.range_end)?;
        if first != next {
            return Ok(false);
        }
        let Some(selected) = validated.chunks().get(first..limit) else {
            return Ok(false);
        };
        let (Some(start), Some(end)) = (selected.first(), selected.last()) else {
            return Ok(false);
        };
        if term.offset != start.unpacked_start()
            || term.length
                != end
                    .unpacked_end()
                    .checked_sub(start.unpacked_start())
                    .ok_or(ServerError::Overflow)?
            || term.packed_start != start.packed_start()
            || term.packed_end != end.packed_end()
        {
            return Ok(false);
        }
        next = limit;
    }
    Ok(next == validated.chunks().len())
}
