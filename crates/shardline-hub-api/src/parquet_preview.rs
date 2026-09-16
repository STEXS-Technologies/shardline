//! Bounded, range-backed Parquet preview reader.
//!
//! The reader implements Parquet's random-access interface over Shardline's
//! object store. It fetches only the footer and column/page ranges requested by
//! the Parquet reader; it never materializes the object as one `Vec<u8>`.

use std::io::{self, Cursor, Read};

use arrow_json::LineDelimitedWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use shardline_protocol::ByteRange;
use shardline_server_core::ServerObjectStore;
use shardline_storage::{ObjectKey, ObjectStore};

use crate::{error::HubApiError, models::DatasetRow};

const MAX_BATCH_ROWS: usize = 256;
const RANGE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone)]
struct RangeReader {
    store: ServerObjectStore,
    key: ObjectKey,
    length: u64,
}

impl Length for RangeReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for RangeReader {
    type T = RangedRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.length {
            return Err(ParquetError::EOF("range start past EOF".into()));
        }
        Ok(RangedRead {
            reader: self.clone(),
            position: start,
            buffer: Cursor::new(Vec::new()),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<bytes::Bytes> {
        if length == 0 {
            return Ok(bytes::Bytes::new());
        }
        let end = start
            .checked_add(length as u64)
            .and_then(|n| n.checked_sub(1))
            .ok_or_else(|| ParquetError::General("range overflow".into()))?;
        if end >= self.length {
            return Err(ParquetError::EOF("range past EOF".into()));
        }
        let range = ByteRange::new(start, end).map_err(|e| ParquetError::General(e.to_string()))?;
        self.store
            .read_range(&self.key, range)
            .map(|bytes| {
                debug_assert_eq!(bytes.len(), length);
                bytes::Bytes::from(bytes)
            })
            .map_err(|e| ParquetError::General(e.to_string()))
    }
}

struct RangedRead {
    reader: RangeReader,
    position: u64,
    buffer: Cursor<Vec<u8>>,
}

impl Read for RangedRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.buffer.position() >= self.buffer.get_ref().len() as u64 {
            if self.position >= self.reader.length {
                return Ok(0);
            }
            let count = (self.reader.length - self.position).min(RANGE_BYTES) as usize;
            let bytes = self
                .reader
                .get_bytes(self.position, count)
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.buffer = Cursor::new(bytes.to_vec());
            self.position += count as u64;
        }
        let result = self.buffer.read(output);
        result
    }
}

/// Read a bounded page of rows from a Parquet object through ranged reads.
pub fn read_rows(
    store: &ServerObjectStore,
    key: ObjectKey,
    size: u64,
    offset: usize,
    limit: usize,
    selected_columns: &[String],
) -> Result<(Vec<String>, Vec<DatasetRow>), HubApiError> {
    let reader = RangeReader {
        store: store.clone(),
        key,
        length: size,
    };
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|e| HubApiError::PathValidation(format!("invalid parquet: {e}")))?
        .with_batch_size(MAX_BATCH_ROWS)
        .with_limit(offset.saturating_add(limit));
    if !selected_columns.is_empty() {
        let mask = ProjectionMask::columns(
            builder.parquet_schema(),
            selected_columns.iter().map(String::as_str),
        );
        builder = builder.with_projection(mask);
    }
    let schema = builder.schema().clone();
    let mut columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let batches = builder
        .build()
        .map_err(|e| HubApiError::PathValidation(format!("invalid parquet: {e}")))?;
    let mut rows = Vec::new();
    for batch in batches {
        let batch =
            batch.map_err(|e| HubApiError::PathValidation(format!("invalid parquet: {e}")))?;
        if !selected_columns.is_empty() {
            columns = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
        }
        let mut encoded = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut encoded);
            writer
                .write(&batch)
                .map_err(|e| HubApiError::PathValidation(format!("invalid parquet row: {e}")))?;
            writer
                .finish()
                .map_err(|e| HubApiError::PathValidation(format!("invalid parquet row: {e}")))?;
        }
        for value in encoded
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            if rows.len() >= limit {
                break;
            }
            let row: std::collections::BTreeMap<String, serde_json::Value> =
                serde_json::from_slice(value).map_err(|e| {
                    HubApiError::PathValidation(format!("invalid parquet row: {e}"))
                })?;
            rows.push(DatasetRow { columns: row });
        }
        if rows.len() >= limit {
            break;
        }
    }
    // Arrow's limit starts at row zero; apply the requested offset after the
    // bounded decode so no unbounded scan or result allocation is possible.
    let skipped = offset.min(rows.len());
    Ok((
        columns,
        rows.into_iter().skip(skipped).take(limit).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reader_rejects_ranges_past_eof() {
        let reader = RangeReader {
            store: ServerObjectStore::Blackhole,
            key: ObjectKey::parse("x").unwrap(),
            length: 10,
        };
        assert!(reader.get_bytes(9, 2).is_err());
    }
}
