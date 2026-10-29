//! Amortize blocking handoffs without waiting for speculative candidate rows.

use super::{admit_record_conversion, run_record_conversion, stored_record_from_row};
use crate::{PostgresMetadataStoreError, PostgresRecordLocator, StoredRecord};
use futures_util::{FutureExt, Stream, TryStreamExt};
use sqlx::{Row, postgres::PgRow};

const MAX_ROWS: usize = 16;
const TARGET_BYTES: usize = 64 * 1024;

#[cfg(test)]
mod tests;

pub(super) struct Batch {
    pub records: Vec<Result<StoredRecord<PostgresRecordLocator>, PostgresMetadataStoreError>>,
    pub error: Option<PostgresMetadataStoreError>,
    pub finished: bool,
}

pub(super) async fn read(
    rows: &mut (impl Stream<Item = Result<PgRow, sqlx::Error>> + Unpin + Send),
) -> Result<Batch, PostgresMetadataStoreError> {
    // Charge the existing global budget before polling any owned row. Each
    // admitted batch has at most 16 rows and 64 KiB plus one oversized row.
    let permit = admit_record_conversion().await?;
    let mut pending = Vec::with_capacity(MAX_ROWS);
    let mut bytes = 0_usize;
    let mut error = None;
    let mut finished = false;
    let mut next = rows.try_next().await;
    loop {
        match next {
            Ok(Some(row)) => {
                for index in 0..row.len() {
                    bytes = bytes.saturating_add(
                        row.try_get_raw(index)
                            .ok()
                            .and_then(|value| value.as_bytes().ok().map(<[u8]>::len))
                            .unwrap_or_default(),
                    );
                }
                pending.push(row);
            }
            Ok(None) => {
                finished = true;
                break;
            }
            Err(cause) => {
                error = Some(cause.into());
                break;
            }
        }
        if pending.len() >= MAX_ROWS || bytes >= TARGET_BYTES {
            break;
        }
        // Never delay a complete proof to wait for a later network row.
        let Some(ready) = rows.try_next().now_or_never() else {
            break;
        };
        next = ready;
    }
    let records = run_record_conversion(permit, move || {
        // Preserve per-record errors in order: a later malformed candidate
        // must not override an earlier complete authoritative proof.
        Ok(pending.iter().map(stored_record_from_row).collect())
    })
    .await?;
    Ok(Batch {
        records,
        error,
        finished,
    })
}
