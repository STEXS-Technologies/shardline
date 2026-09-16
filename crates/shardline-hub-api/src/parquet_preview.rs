//! Bounded, range-backed Parquet preview reader.
//!
//! The reader implements Parquet's random-access interface over Shardline's
//! object store. It fetches only the footer and column/page ranges requested by
//! the Parquet reader; it never materializes the object as one `Vec<u8>`.

use std::{
    collections::HashMap,
    io::{self, Cursor, Read},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_json::LineDelimitedWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use shardline_protocol::ByteRange;
use shardline_server_core::ServerObjectStore;
use shardline_storage::{ObjectKey, ObjectStore};

use crate::{
    error::HubApiError,
    models::DatasetRow,
    query::{Aggregate, AggregateFunction, OrderTerm, Predicate, PredicateOp, Scalar},
};

const MAX_BATCH_ROWS: usize = 256;
const RANGE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SCANNED_BYTES: u64 = 128 * 1024 * 1024;
const MAX_QUERY_SCAN_ROWS: usize = 100_000;
const MAX_RESULT_BYTES: usize = 16 * 1024 * 1024;
// Keep each decoded Arrow batch bounded even when a Parquet page contains
// unusually large values. The result budget alone is insufficient because
// rows rejected by predicates must still be decoded before they are dropped.
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_GET_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONCURRENT_QUERIES: usize = 8;
static QUERY_ADMISSION: OnceLock<Mutex<AdmissionState>> = OnceLock::new();

const MAX_QUERIES_PER_TENANT: usize = 2;

fn invalid_parquet_error() -> HubApiError {
    // Keep parser/storage details out of the client response. They may contain
    // object keys, local paths, backend URLs, or provider error text.
    HubApiError::PathValidation("invalid parquet input".to_owned())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryFailureClass {
    ScanLimit,
    ResultLimit,
    Admission,
    InvalidInput,
    Worker,
    Validation,
}

impl QueryFailureClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ScanLimit => "scan_limit",
            Self::ResultLimit => "result_limit",
            Self::Admission => "admission",
            Self::InvalidInput => "invalid_input",
            Self::Worker => "worker",
            Self::Validation => "validation",
        }
    }
}

#[derive(Debug)]
pub(crate) struct QueryFailure {
    pub(crate) error: HubApiError,
    pub(crate) class: QueryFailureClass,
}

impl QueryFailure {
    pub(crate) const fn new(error: HubApiError, class: QueryFailureClass) -> Self {
        Self { error, class }
    }
}

impl From<HubApiError> for QueryFailure {
    fn from(error: HubApiError) -> Self {
        Self::new(error, QueryFailureClass::Validation)
    }
}

#[derive(Default)]
struct AdmissionState {
    active: usize,
    by_tenant: HashMap<String, usize>,
}

struct AdmissionGuard {
    tenant: String,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = QUERY_ADMISSION
            .get_or_init(|| Mutex::new(AdmissionState::default()))
            .lock()
        {
            state.active = state.active.saturating_sub(1);
            if let Some(active) = state.by_tenant.get_mut(&self.tenant) {
                *active = active.saturating_sub(1);
                if *active == 0 {
                    state.by_tenant.remove(&self.tenant);
                }
            }
        }
    }
}

fn admit_query(tenant: &str) -> Result<AdmissionGuard, HubApiError> {
    let mut state = QUERY_ADMISSION
        .get_or_init(|| Mutex::new(AdmissionState::default()))
        .lock()
        .map_err(|_poisoned| {
            HubApiError::PathValidation("query admission unavailable".to_owned())
        })?;
    let tenant_active = state.by_tenant.get(tenant).copied().unwrap_or(0);
    if state.active >= MAX_CONCURRENT_QUERIES || tenant_active >= MAX_QUERIES_PER_TENANT {
        shardline_metrics::metrics().query.admissions_rejected.inc();
        return Err(HubApiError::PathValidation(
            "query concurrency limit exceeded".to_owned(),
        ));
    }
    state.active = state.active.saturating_add(1);
    state
        .by_tenant
        .entry(tenant.to_owned())
        .and_modify(|active| *active = active.saturating_add(1))
        .or_insert(1);
    Ok(AdmissionGuard {
        tenant: tenant.to_owned(),
    })
}

#[derive(Clone)]
struct RangeReader {
    store: ServerObjectStore,
    key: ObjectKey,
    length: u64,
    scanned: Arc<AtomicU64>,
    cancelled: Arc<AtomicBool>,
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
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(ParquetError::General("query cancelled".into()));
        }
        if length == 0 {
            return Ok(bytes::Bytes::new());
        }
        if length > MAX_GET_BYTES {
            return Err(ParquetError::General(
                "parquet range request exceeds limit".into(),
            ));
        }
        let end = start
            .checked_add(length as u64)
            .and_then(|n| n.checked_sub(1))
            .ok_or_else(|| ParquetError::General("range overflow".into()))?;
        if end >= self.length {
            return Err(ParquetError::EOF("range past EOF".into()));
        }
        let mut output = Vec::with_capacity(length);
        let mut chunk_start = start;
        let mut remaining = length;
        while remaining > 0 {
            if self.cancelled.load(Ordering::Relaxed) {
                return Err(ParquetError::General("query cancelled".into()));
            }
            let chunk_length = remaining.min(RANGE_BYTES as usize);
            let chunk_end = chunk_start
                .checked_add(chunk_length as u64)
                .and_then(|n| n.checked_sub(1))
                .ok_or_else(|| ParquetError::General("range overflow".into()))?;
            let range = ByteRange::new(chunk_start, chunk_end)
                .map_err(|e| ParquetError::General(e.to_string()))?;
            shardline_metrics::metrics().query.range_requests.inc();
            let scanned = self
                .scanned
                .fetch_add(chunk_length as u64, Ordering::Relaxed)
                .saturating_add(chunk_length as u64);
            if scanned > MAX_SCANNED_BYTES {
                shardline_metrics::metrics().query.scan_limit_rejected.inc();
                return Err(ParquetError::General("parquet scan limit exceeded".into()));
            }
            shardline_metrics::metrics()
                .query
                .scanned_bytes
                .inc_by(chunk_length as u64);
            let bytes = self
                .store
                .read_range(&self.key, range)
                .map_err(|e| ParquetError::General(e.to_string()))?;
            if bytes.len() != chunk_length {
                return Err(ParquetError::General("range length mismatch".into()));
            }
            output.extend_from_slice(&bytes);
            chunk_start = chunk_start.saturating_add(chunk_length as u64);
            remaining = remaining.saturating_sub(chunk_length);
        }
        Ok(bytes::Bytes::from(output))
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
            let remaining = self.reader.length.saturating_sub(self.position);
            let count = remaining.min(RANGE_BYTES) as usize;
            let bytes = self
                .reader
                .get_bytes(self.position, count)
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.buffer = Cursor::new(bytes.to_vec());
            self.position = self.position.saturating_add(count as u64);
        }
        self.buffer.read(output)
    }
}

/// Read a bounded page of rows from a Parquet object through ranged reads.
#[allow(clippy::too_many_arguments)]
pub fn read_rows(
    store: &ServerObjectStore,
    key: ObjectKey,
    size: u64,
    tenant: &str,
    offset: usize,
    limit: usize,
    selected_columns: &[String],
    predicates: &[Predicate],
    aggregates: &[Aggregate],
    order_by: &[OrderTerm],
    cancelled: Arc<AtomicBool>,
) -> Result<(Vec<String>, Vec<DatasetRow>), QueryFailure> {
    shardline_metrics::metrics().query.requests.inc();
    let started = Instant::now();
    let _admission = admit_query(tenant)
        .map_err(|error| QueryFailure::new(error, QueryFailureClass::Admission))?;
    // Admission is fail-fast today (there is no unbounded waiter queue), so
    // successful requests have zero queue delay. Keep the histogram explicit
    // so a future fair scheduler can populate the same metric without a
    // contract change.
    shardline_metrics::metrics()
        .query
        .queue_seconds
        .observe(0.0);
    let reader = RangeReader {
        store: store.clone(),
        key,
        length: size,
        scanned: Arc::new(AtomicU64::new(0)),
        cancelled,
    };
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|_error| {
            QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
        })?
        .with_batch_size(MAX_BATCH_ROWS)
        .with_limit(
            if predicates.is_empty() && aggregates.is_empty() && order_by.is_empty() {
                offset.saturating_add(limit)
            } else {
                MAX_QUERY_SCAN_ROWS.saturating_add(1)
            },
        );
    let schema_fields = builder.parquet_schema().root_schema().get_fields();
    for column in selected_columns {
        if !schema_fields.iter().any(|field| field.name() == column) {
            return Err(QueryFailure::new(
                HubApiError::PathValidation("query references an unknown column".to_owned()),
                QueryFailureClass::Validation,
            ));
        }
    }
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
    let batches = builder.build().map_err(|_error| {
        QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
    })?;
    let mut rows = Vec::new();
    let mut scanned_rows = 0usize;
    let mut result_bytes = 0usize;
    for batch in batches {
        let batch = batch.map_err(|_error| {
            QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
        })?;
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
            writer.write(&batch).map_err(|_error| {
                QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
            })?;
            writer.finish().map_err(|_error| {
                QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
            })?;
        }
        if encoded.len() > MAX_BATCH_BYTES {
            shardline_metrics::metrics()
                .query
                .result_limit_rejected
                .inc();
            return Err(QueryFailure::new(
                HubApiError::PathValidation("query batch memory limit exceeded".to_owned()),
                QueryFailureClass::ResultLimit,
            ));
        }
        for value in encoded
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let row: std::collections::BTreeMap<String, serde_json::Value> =
                serde_json::from_slice(value).map_err(|_error| {
                    QueryFailure::new(invalid_parquet_error(), QueryFailureClass::InvalidInput)
                })?;
            scanned_rows = scanned_rows.saturating_add(1);
            if scanned_rows > MAX_QUERY_SCAN_ROWS {
                shardline_metrics::metrics().query.scan_limit_rejected.inc();
                return Err(QueryFailure::new(
                    HubApiError::PathValidation("query row scan limit exceeded".to_owned()),
                    QueryFailureClass::ScanLimit,
                ));
            }
            if predicates
                .iter()
                .all(|predicate| predicate_matches(&row, predicate))
            {
                result_bytes = result_bytes.saturating_add(value.len());
                if result_bytes > MAX_RESULT_BYTES {
                    shardline_metrics::metrics()
                        .query
                        .result_limit_rejected
                        .inc();
                    return Err(QueryFailure::new(
                        HubApiError::PathValidation("query result limit exceeded".to_owned()),
                        QueryFailureClass::ResultLimit,
                    ));
                }
                rows.push(DatasetRow { columns: row });
            }
        }
    }
    if !aggregates.is_empty() {
        let mut aggregate_row = std::collections::BTreeMap::new();
        for aggregate in aggregates {
            let alias = aggregate
                .alias
                .clone()
                .unwrap_or_else(|| format!("{:?}", aggregate.function).to_lowercase());
            let value = aggregate_value(&rows, aggregate).map_err(QueryFailure::from)?;
            aggregate_row.insert(alias, value);
        }
        shardline_metrics::metrics().query.returned_rows.inc();
        shardline_metrics::metrics()
            .query
            .returned_bytes
            .inc_by(result_bytes as u64);
        shardline_metrics::metrics()
            .query
            .execution_seconds
            .observe(started.elapsed().as_secs_f64());
        return Ok((
            aggregate_row.keys().cloned().collect(),
            vec![DatasetRow {
                columns: aggregate_row,
            }],
        ));
    }
    if !order_by.is_empty() {
        rows.sort_by(|left, right| {
            for term in order_by {
                let ordering = compare_values(
                    left.columns.get(&term.column),
                    right.columns.get(&term.column),
                );
                if ordering != std::cmp::Ordering::Equal {
                    return if term.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    // Arrow's limit starts at row zero; apply the requested offset after the
    // bounded decode so no unbounded scan or result allocation is possible.
    let skipped = offset.min(rows.len());
    let result_rows = rows.len().saturating_sub(skipped).min(limit);
    shardline_metrics::metrics()
        .query
        .returned_rows
        .inc_by(result_rows as u64);
    shardline_metrics::metrics()
        .query
        .returned_bytes
        .inc_by(result_bytes as u64);
    shardline_metrics::metrics()
        .query
        .execution_seconds
        .observe(started.elapsed().as_secs_f64());
    Ok((
        columns,
        rows.into_iter().skip(skipped).take(limit).collect(),
    ))
}

fn predicate_matches(
    row: &std::collections::BTreeMap<String, serde_json::Value>,
    predicate: &Predicate,
) -> bool {
    let value = row.get(&predicate.column);
    match predicate.op {
        PredicateOp::IsNull => value.is_none_or(serde_json::Value::is_null),
        PredicateOp::IsNotNull => value.is_some_and(|v| !v.is_null()),
        PredicateOp::Eq => value_matches(value, &predicate.value, |a, b| a == b),
        PredicateOp::NotEq => value_matches(value, &predicate.value, |a, b| a != b),
        PredicateOp::Lt => ordered_matches(value, &predicate.value, std::cmp::Ordering::is_lt),
        PredicateOp::Lte => ordered_matches(value, &predicate.value, std::cmp::Ordering::is_le),
        PredicateOp::Gt => ordered_matches(value, &predicate.value, std::cmp::Ordering::is_gt),
        PredicateOp::Gte => ordered_matches(value, &predicate.value, std::cmp::Ordering::is_ge),
    }
}

fn value_matches<F: FnOnce(&serde_json::Value, &serde_json::Value) -> bool>(
    value: Option<&serde_json::Value>,
    scalar: &Scalar,
    compare: F,
) -> bool {
    value.is_some_and(|value| compare(value, &scalar_to_json(scalar)))
}

fn ordered_matches<F: FnOnce(std::cmp::Ordering) -> bool>(
    value: Option<&serde_json::Value>,
    scalar: &Scalar,
    compare: F,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    let other = scalar_to_json(scalar);
    let ordering = match (value, &other) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            match (a.as_f64(), b.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => None,
            }
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    };
    ordering.is_some_and(compare)
}

fn compare_values(
    left: Option<&serde_json::Value>,
    right: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(serde_json::Value::Number(a)), Some(serde_json::Value::Number(b))) => {
            match (a.as_f64(), b.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
                _ => std::cmp::Ordering::Equal,
            }
        }
        (Some(serde_json::Value::String(a)), Some(serde_json::Value::String(b))) => a.cmp(b),
        (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    }
}

fn scalar_to_json(scalar: &Scalar) -> serde_json::Value {
    match scalar {
        Scalar::Null => serde_json::Value::Null,
        Scalar::Bool(value) => serde_json::Value::Bool(*value),
        Scalar::Integer(value) => serde_json::json!(value),
        Scalar::Float(value) => value
            .parse::<f64>()
            .map_or(serde_json::Value::Null, |v| serde_json::json!(v)),
        Scalar::Text(value) => serde_json::Value::String(value.clone()),
    }
}

#[allow(clippy::float_arithmetic)]
fn aggregate_value(
    rows: &[DatasetRow],
    aggregate: &Aggregate,
) -> Result<serde_json::Value, HubApiError> {
    if matches!(aggregate.function, AggregateFunction::Count) {
        return Ok(serde_json::json!(rows.len()));
    }
    let column = aggregate
        .column
        .as_deref()
        .ok_or_else(|| HubApiError::PathValidation("aggregate column required".to_owned()))?;
    let values: Vec<f64> = rows
        .iter()
        .filter_map(|row| row.columns.get(column).and_then(serde_json::Value::as_f64))
        .collect();
    if values.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    let value = match aggregate.function {
        AggregateFunction::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggregateFunction::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggregateFunction::Sum => values.iter().sum(),
        AggregateFunction::Avg => values.iter().sum::<f64>() / values.len() as f64,
        AggregateFunction::Count => 0.0,
    };
    Ok(serde_json::json!(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_ADMISSION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn admission_test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_ADMISSION_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap()
    }

    #[test]
    fn reader_rejects_ranges_past_eof() {
        let reader = RangeReader {
            store: ServerObjectStore::Blackhole,
            key: ObjectKey::parse("x").unwrap(),
            length: 10,
            scanned: Arc::new(AtomicU64::new(0)),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        assert!(reader.get_bytes(9, 2).is_err());
    }

    #[test]
    fn reader_rejects_scanned_byte_budget() {
        let scanned = Arc::new(AtomicU64::new(MAX_SCANNED_BYTES));
        let reader = RangeReader {
            store: ServerObjectStore::Blackhole,
            key: ObjectKey::parse("x").unwrap(),
            length: 10,
            scanned,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        assert!(reader.get_bytes(0, 1).is_err());
    }

    #[test]
    fn reader_stops_after_cancellation() {
        let reader = RangeReader {
            store: ServerObjectStore::Blackhole,
            key: ObjectKey::parse("x").unwrap(),
            length: 10,
            scanned: Arc::new(AtomicU64::new(0)),
            cancelled: Arc::new(AtomicBool::new(true)),
        };
        assert!(reader.get_bytes(0, 1).is_err());
    }

    #[test]
    fn reader_rejects_ranges_larger_than_chunk_budget() {
        let reader = RangeReader {
            store: ServerObjectStore::Blackhole,
            key: ObjectKey::parse("x").unwrap(),
            length: (MAX_GET_BYTES as u64).saturating_add(1),
            scanned: Arc::new(AtomicU64::new(0)),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        assert!(
            reader
                .get_bytes(0, MAX_GET_BYTES.saturating_add(1))
                .is_err()
        );
    }

    #[test]
    fn admission_is_bounded() {
        let _lock = admission_test_lock();
        let guards: Vec<_> = (0..MAX_CONCURRENT_QUERIES)
            .map(|index| admit_query(&format!("tenant-{index}")).unwrap())
            .collect();
        assert!(admit_query("another-tenant").is_err());
        drop(guards);
        assert!(admit_query("another-tenant").is_ok());
    }

    #[test]
    fn admission_limits_each_tenant_before_global_limit() {
        let _lock = admission_test_lock();
        let guards = [
            admit_query("same-tenant").unwrap(),
            admit_query("same-tenant").unwrap(),
        ];
        assert!(admit_query("same-tenant").is_err());
        drop(guards);
    }

    #[test]
    fn admission_preserves_capacity_for_other_tenants() {
        let _lock = admission_test_lock();
        let same_tenant = [
            admit_query("busy-tenant").unwrap(),
            admit_query("busy-tenant").unwrap(),
        ];
        // A noisy tenant cannot consume the global budget: another tenant is
        // admitted immediately while the first tenant is at its per-tenant
        // ceiling. This is the fail-fast fairness guarantee used in lieu of
        // an unbounded waiter queue.
        let other = admit_query("other-tenant").unwrap();
        drop(other);
        drop(same_tenant);
    }
}
