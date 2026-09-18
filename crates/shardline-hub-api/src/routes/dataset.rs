use axum::{
    Json,
    extract::{Path, Query, State},
};
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectKey, ObjectStore};

use crate::query::DatasetQueryRequest;
use crate::{error::HubApiError, models::*};
use shardline_index::hub::{HubFileEntry, HubRepoType};

use super::{HubRepository, HubState, lfs_object_key};

struct QueryCancellationGuard {
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    armed: bool,
}

impl QueryCancellationGuard {
    const fn new(cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            cancelled,
            armed: true,
        }
    }

    fn cancel(&mut self, reason: &'static str) {
        if !self.armed {
            return;
        }
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        shardline_metrics::metrics().query.cancellations.inc();
        shardline_metrics::metrics()
            .query
            .cancellation_reasons
            .with_label_values(&[reason])
            .inc();
        self.armed = false;
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for QueryCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel("client_disconnect");
        }
    }
}

fn record_query_failure(class: crate::parquet_preview::QueryFailureClass) {
    shardline_metrics::metrics().query.failures.inc();
    shardline_metrics::metrics()
        .query
        .failure_classes
        .with_label_values(&[class.as_str()])
        .inc();
}

/// Bounds for the range-backed CSV/JSONL preview path. Text rows are decoded
/// one line at a time; the complete object is never materialized.
const MAX_DATASET_TEXT_SCAN_BYTES: u64 = 128 * 1024 * 1024;
const MAX_DATASET_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_DATASET_TEXT_RESULT_BYTES: usize = 16 * 1024 * 1024;

// ---- Dataset viewer endpoints ----

/// Lists parquet/data files in a dataset repository.
pub(crate) async fn dataset_parquet(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((ns, repo_name)): Path<(String, String)>,
) -> Result<Json<DatasetParquetResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_parquet", "GET", 200);
    let name = format!("{ns}/{repo_name}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let parquet_files: Vec<DatasetParquetFile> = files
        .iter()
        .filter(|f| {
            f.path.ends_with(".parquet") || f.path.ends_with(".csv") || f.path.ends_with(".jsonl")
        })
        .map(|f| DatasetParquetFile {
            path: f.path.clone(),
            size: f.size,
            sha: f.sha.clone(),
        })
        .collect();
    Ok(Json(DatasetParquetResponse {
        files: parquet_files,
    }))
}

/// Returns the first rows of a dataset split.
pub(crate) async fn dataset_first_rows(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name)): Path<(String, String)>,
    Query(query): Query<DatasetFirstRowsQuery>,
) -> Result<Json<DatasetFirstRowsResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_first_rows", "GET", 200);
    let name = format!("{ns}/{repo_name}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let data_file = match find_dataset_file(&files, &query.config, &query.split) {
        Some(f) => f,
        None => {
            // Empty dataset — return 200 with empty rows (per HuggingFace Hub API spec).
            return Ok(Json(DatasetFirstRowsResponse {
                columns: vec![],
                rows: vec![],
            }));
        }
    };
    let limit = query.limit.min(1000);
    let (columns, rows) =
        read_dataset_rows_async(&state, data_file, repo.capability(), &name, 0, limit).await?;
    Ok(Json(DatasetFirstRowsResponse { columns, rows }))
}

/// Returns rows from a dataset split with pagination.
pub(crate) async fn dataset_viewer(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name, split)): Path<(String, String, String)>,
    Query(query): Query<DatasetViewerQuery>,
) -> Result<Json<DatasetViewerResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_viewer", "GET", 200);
    let name = format!("{ns}/{repo_name}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let data_file = find_dataset_file(&files, &query.config, &split).ok_or_else(|| {
        HubApiError::PathValidation("no data file found for config/split".to_owned())
    })?;
    let length = query.length.min(10000);
    let (columns, rows) = read_dataset_rows_async(
        &state,
        data_file,
        repo.capability(),
        &name,
        query.offset,
        length,
    )
    .await?;
    Ok(Json(DatasetViewerResponse {
        columns,
        rows,
        num_rows_total: None,
    }))
}

/// Execute the bounded, revision-pinned query contract against one Parquet file.
/// The native reader uses bounded range requests and a blocking worker; it does
/// not materialize the complete object in the async API process.
pub(crate) async fn dataset_query(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name)): Path<(String, String)>,
    Json(request): Json<DatasetQueryRequest>,
) -> Result<Json<DatasetViewerResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_query", "POST", 200);
    request
        .validate()
        .map_err(|e| HubApiError::PathValidation(e.to_string()))?;
    let name = format!("{ns}/{repo_name}");
    if request.repository != name {
        // Keep the identity in the body and URL bound together.  This prevents
        // callers from presenting a valid file identity for another repository
        // while relying on the route path for authorization.
        return Err(HubApiError::RevisionNotFound);
    }
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let revision = state
        .store
        .resolve_revision(&name, &request.revision)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    if revision != request.revision {
        return Err(HubApiError::RevisionNotFound);
    }
    let files = state
        .store
        .get_files(&revision)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let file = find_dataset_file(&files, &request.config, &request.split).ok_or_else(|| {
        HubApiError::PathValidation("no data file found for config/split".to_owned())
    })?;
    if file.sha != request.file_sha {
        return Err(HubApiError::RevisionNotFound);
    }
    if !file.path.ends_with(".parquet") {
        return Err(HubApiError::PathValidation(
            "structured query requires parquet".to_owned(),
        ));
    }
    let key = lfs_object_key(&file.sha, repo.capability())
        .map_err(|e| HubApiError::PathValidation(e.to_string()))?;
    let size = state
        .object_store
        .metadata(&key)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::NotFound)?
        .length();
    let object_store = state.object_store.clone();
    let mut selected_columns = request.columns.clone();
    for predicate in &request.predicates {
        if !selected_columns
            .iter()
            .any(|column| column == &predicate.column)
        {
            selected_columns.push(predicate.column.clone());
        }
    }
    for aggregate in &request.aggregates {
        if let Some(column) = &aggregate.column
            && !selected_columns.iter().any(|selected| selected == column)
        {
            selected_columns.push(column.clone());
        }
    }
    for order in &request.order_by {
        if !selected_columns
            .iter()
            .any(|selected| selected == &order.column)
        {
            selected_columns.push(order.column.clone());
        }
    }
    let predicates = request.predicates.clone();
    let aggregates = request.aggregates.clone();
    let order_by = request.order_by.clone();
    let offset = request.offset as usize;
    let limit = request.limit as usize;
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cancellation_guard = QueryCancellationGuard::new(cancelled.clone());
    let worker_cancelled = cancelled.clone();
    let read = tokio::task::spawn_blocking(move || {
        crate::parquet_preview::read_rows(
            &object_store,
            key,
            size,
            &name,
            offset,
            limit,
            &selected_columns,
            &predicates,
            &aggregates,
            &order_by,
            worker_cancelled,
        )
    });
    let worker_result = match tokio::time::timeout(std::time::Duration::from_secs(30), read).await {
        Ok(join_result) => {
            cancellation_guard.disarm();
            join_result.map_err(|_join_error| {
                crate::parquet_preview::QueryFailure::new(
                    HubApiError::PathValidation("query worker failed".to_owned()),
                    crate::parquet_preview::QueryFailureClass::Worker,
                )
            })
        }
        Err(_timeout_error) => {
            cancellation_guard.cancel("deadline");
            Err(crate::parquet_preview::QueryFailure::new(
                HubApiError::PathValidation("query deadline exceeded".to_owned()),
                crate::parquet_preview::QueryFailureClass::Worker,
            ))
        }
    };
    let (output_columns, rows) = match worker_result {
        Ok(Ok(result)) => result,
        Ok(Err(failure)) | Err(failure) => {
            record_query_failure(failure.class);
            return Err(failure.error);
        }
    };
    let (output_columns, rows) = if request.aggregates.is_empty() && !request.columns.is_empty() {
        let mut rows = rows;
        for row in &mut rows {
            row.columns
                .retain(|column, _| request.columns.iter().any(|selected| selected == column));
        }
        (request.columns.clone(), rows)
    } else {
        (output_columns, rows)
    };
    Ok(Json(DatasetViewerResponse {
        columns: output_columns,
        rows,
        num_rows_total: None,
    }))
}

/// Finds the data file for a given config and split.
pub(crate) fn find_dataset_file<'input>(
    files: &'input [HubFileEntry],
    config: &str,
    split: &str,
) -> Option<&'input HubFileEntry> {
    let candidates = [
        format!("{config}/{split}/data.parquet"),
        format!("{config}/{split}/data.csv"),
        format!("{config}/{split}/data.jsonl"),
        format!("data/{split}/data.parquet"),
        format!("data/{split}/data.csv"),
        format!("data/{split}/data.jsonl"),
        format!("{split}/data.parquet"),
        format!("{split}/data.csv"),
        format!("{split}/data.jsonl"),
        String::from("data.parquet"),
        String::from("data.csv"),
        String::from("data.jsonl"),
    ];
    for candidate in &candidates {
        if let Some(file) = files.iter().find(|f| f.path == *candidate) {
            return Some(file);
        }
    }
    None
}

/// Parses rows from inline file content (CSV or JSONL).
#[cfg(test)]
pub(crate) fn parse_rows_from_content(
    content: &[u8],
    path: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<DatasetRow>, HubApiError> {
    let text = std::str::from_utf8(content)
        .map_err(|e| HubApiError::PathValidation(format!("invalid UTF-8: {e}")))?;
    if path.ends_with(".jsonl") {
        parse_jsonl_rows(text, offset, limit)
    } else if path.ends_with(".csv") {
        parse_csv_rows(text, offset, limit)
    } else {
        Err(HubApiError::PathValidation(format!(
            "unsupported file format: {path}"
        )))
    }
}

/// Parses JSONL (newline-delimited JSON) rows.
#[cfg(test)]
pub(crate) fn parse_jsonl_rows(
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<DatasetRow>, HubApiError> {
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i < offset {
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let line_number = i
            .checked_add(1)
            .ok_or_else(|| HubApiError::PathValidation("line number overflow".to_owned()))?;
        let value: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            HubApiError::PathValidation(format!("invalid JSON at line {}: {e}", line_number))
        })?;
        let columns = value
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        rows.push(DatasetRow { columns });
    }
    Ok(rows)
}

/// Parses CSV rows, handling quoted fields that may contain commas.
#[cfg(test)]
pub(crate) fn parse_csv_rows(
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<DatasetRow>, HubApiError> {
    let mut lines = text.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| HubApiError::PathValidation("empty CSV file".to_owned()))?;
    let headers: Vec<String> = parse_csv_line(header_line)
        .into_iter()
        .map(|h| h.trim().trim_matches('"').to_owned())
        .collect();
    let mut rows = Vec::new();
    for (i, line) in lines.enumerate() {
        if i < offset {
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let values: Vec<&str> = parse_csv_line(line);
        let columns: std::collections::BTreeMap<String, serde_json::Value> = headers
            .iter()
            .zip(values.iter())
            .map(|(h, v)| {
                let json_val = serde_json::from_str(v)
                    .unwrap_or_else(|_| serde_json::Value::String(v.trim_matches('"').to_owned()));
                (h.clone(), json_val)
            })
            .collect();
        rows.push(DatasetRow { columns });
    }
    Ok(rows)
}

fn read_dataset_rows(
    state: &HubState,
    file: &HubFileEntry,
    auth: &AuthorizedRepository,
    tenant: &str,
    offset: usize,
    limit: usize,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(Vec<String>, Vec<DatasetRow>), HubApiError> {
    let key = lfs_object_key(&file.sha, auth)
        .map_err(|error| HubApiError::PathValidation(error.to_string()))?;
    let size = state
        .object_store
        .metadata(&key)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .ok_or(HubApiError::NotFound)?
        .length();
    if file.path.ends_with(".parquet") {
        return crate::parquet_preview::read_rows(
            &state.object_store,
            key,
            size,
            tenant,
            offset,
            limit,
            &[],
            &[],
            &[],
            &[],
            cancelled,
        )
        .map_err(|failure| failure.error);
    }
    read_text_rows_streaming(
        &state.object_store,
        &key,
        size,
        &file.path,
        offset,
        limit,
        cancelled,
    )
}

/// Range-backed line reader used by text previews. It retains one storage
/// chunk and the current line, never the complete object.
struct RangeLineReader {
    store: shardline_server_core::ServerObjectStore,
    key: ObjectKey,
    length: u64,
    position: u64,
    scanned: u64,
    chunk: Vec<u8>,
    chunk_position: usize,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RangeLineReader {
    fn new(
        store: &shardline_server_core::ServerObjectStore,
        key: ObjectKey,
        length: u64,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            store: store.clone(),
            key,
            length,
            position: 0,
            scanned: 0,
            chunk: Vec::new(),
            chunk_position: 0,
            cancelled,
        }
    }

    fn refill(&mut self) -> Result<bool, HubApiError> {
        if self.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(HubApiError::PathValidation(
                "dataset preview cancelled".to_owned(),
            ));
        }
        if self.position >= self.length {
            return Ok(false);
        }
        if self.scanned >= MAX_DATASET_TEXT_SCAN_BYTES {
            return Err(HubApiError::PathValidation(
                "dataset text scan limit exceeded".to_owned(),
            ));
        }
        let end_exclusive = self
            .position
            .saturating_add(super::object_io::OBJECT_STREAM_CHUNK_BYTES)
            .min(self.length)
            .min(
                self.position
                    .saturating_add(MAX_DATASET_TEXT_SCAN_BYTES.saturating_sub(self.scanned)),
            );
        let end = end_exclusive
            .checked_sub(1)
            .ok_or_else(|| HubApiError::PathValidation("dataset text range overflow".to_owned()))?;
        let range = shardline_protocol::ByteRange::new(self.position, end).map_err(|_error| {
            HubApiError::PathValidation("dataset text range overflow".to_owned())
        })?;
        let bytes = self
            .store
            .read_range(&self.key, range)
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        let expected =
            usize::try_from(end_exclusive.saturating_sub(self.position)).map_err(|_error| {
                HubApiError::PathValidation("dataset text range overflow".to_owned())
            })?;
        if bytes.len() != expected || bytes.is_empty() {
            return Err(HubApiError::CasError(
                shardline_server_core::ServerObjectStoreError::StoredObjectLengthMismatch
                    .to_string(),
            ));
        }
        self.position = end_exclusive;
        self.scanned = self.scanned.saturating_add(bytes.len() as u64);
        self.chunk = bytes;
        self.chunk_position = 0;
        Ok(true)
    }

    fn next_line(&mut self) -> Result<Option<Vec<u8>>, HubApiError> {
        let mut line = Vec::new();
        loop {
            if self.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(HubApiError::PathValidation(
                    "dataset preview cancelled".to_owned(),
                ));
            }
            if self.chunk_position >= self.chunk.len() && !self.refill()? {
                return if line.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(line))
                };
            }
            let remaining = self.chunk.get(self.chunk_position..).ok_or_else(|| {
                HubApiError::PathValidation("dataset text cursor out of bounds".to_owned())
            })?;
            if let Some(newline) = remaining.iter().position(|byte| *byte == b'\n') {
                let end = self.chunk_position.saturating_add(newline);
                let segment = self.chunk.get(self.chunk_position..end).ok_or_else(|| {
                    HubApiError::PathValidation("dataset text cursor out of bounds".to_owned())
                })?;
                if line.len().saturating_add(segment.len()) > MAX_DATASET_LINE_BYTES {
                    return Err(HubApiError::PathValidation(
                        "dataset text line exceeds limit".to_owned(),
                    ));
                }
                line.extend_from_slice(segment);
                self.chunk_position = end.saturating_add(1);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }
            if line.len().saturating_add(remaining.len()) > MAX_DATASET_LINE_BYTES {
                return Err(HubApiError::PathValidation(
                    "dataset text line exceeds limit".to_owned(),
                ));
            }
            line.extend_from_slice(remaining);
            self.chunk_position = self.chunk.len();
        }
    }
}

fn read_text_rows_streaming(
    store: &shardline_server_core::ServerObjectStore,
    key: &ObjectKey,
    size: u64,
    path: &str,
    offset: usize,
    limit: usize,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(Vec<String>, Vec<DatasetRow>), HubApiError> {
    let mut reader = RangeLineReader::new(store, key.clone(), size, cancelled);
    let is_csv = path.ends_with(".csv");
    let mut columns = Vec::new();
    if is_csv {
        let header = reader
            .next_line()?
            .ok_or_else(|| HubApiError::PathValidation("empty CSV file".to_owned()))?;
        let header = std::str::from_utf8(&header).map_err(|_error| {
            HubApiError::PathValidation("invalid UTF-8 in dataset text".to_owned())
        })?;
        columns = parse_csv_line(header)
            .into_iter()
            .map(|value| value.trim().trim_matches('"').to_owned())
            .collect();
    }
    let mut rows = Vec::new();
    let mut data_row = 0usize;
    let mut result_bytes = 0usize;
    while let Some(line) = reader.next_line()? {
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        if data_row < offset {
            data_row = data_row.saturating_add(1);
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        result_bytes = result_bytes.saturating_add(line.len());
        if result_bytes > MAX_DATASET_TEXT_RESULT_BYTES {
            return Err(HubApiError::PathValidation(
                "dataset text result limit exceeded".to_owned(),
            ));
        }
        let line = std::str::from_utf8(&line).map_err(|_error| {
            HubApiError::PathValidation("invalid UTF-8 in dataset text".to_owned())
        })?;
        let row = if is_csv {
            let values = parse_csv_line(line);
            let mapped = columns
                .iter()
                .zip(values.iter())
                .map(|(header, value)| {
                    let json_value = serde_json::from_str(value).unwrap_or_else(|_| {
                        serde_json::Value::String(value.trim_matches('"').to_owned())
                    });
                    (header.clone(), json_value)
                })
                .collect();
            DatasetRow { columns: mapped }
        } else {
            let value: serde_json::Value = serde_json::from_str(line).map_err(|_error| {
                HubApiError::PathValidation("invalid JSON in dataset text".to_owned())
            })?;
            let mapped = value
                .as_object()
                .map(|object| {
                    object
                        .iter()
                        .map(|(field, field_value)| (field.clone(), field_value.clone()))
                        .collect()
                })
                .unwrap_or_default();
            DatasetRow { columns: mapped }
        };
        if columns.is_empty() {
            columns = row.columns.keys().cloned().collect();
            columns.sort();
        }
        rows.push(row);
        data_row = data_row.saturating_add(1);
    }
    columns.sort();
    Ok((columns, rows))
}

async fn read_dataset_rows_async(
    state: &HubState,
    file: &HubFileEntry,
    auth: &AuthorizedRepository,
    tenant: &str,
    offset: usize,
    limit: usize,
) -> Result<(Vec<String>, Vec<DatasetRow>), HubApiError> {
    let state = state.clone();
    let file = file.clone();
    let auth = auth.clone();
    let tenant = tenant.to_owned();
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_cancelled = cancelled.clone();
    let mut cancellation_guard = QueryCancellationGuard::new(cancelled);
    let worker = tokio::task::spawn_blocking(move || {
        read_dataset_rows(
            &state,
            &file,
            &auth,
            &tenant,
            offset,
            limit,
            worker_cancelled,
        )
    });
    match tokio::time::timeout(std::time::Duration::from_secs(30), worker).await {
        Ok(join_result) => {
            cancellation_guard.disarm();
            join_result.map_err(|_join_error| {
                HubApiError::PathValidation("dataset preview worker failed".to_owned())
            })?
        }
        Err(_timeout_error) => {
            cancellation_guard.cancel("deadline");
            Err(HubApiError::PathValidation(
                "dataset preview deadline exceeded".to_owned(),
            ))
        }
    }
}

/// Parses a single CSV line, respecting double-quoted fields that may contain
/// commas and escaped quotes (`""`).
pub(crate) fn parse_csv_line(line: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut current = line;
    loop {
        if current.is_empty() {
            break;
        }
        if current.starts_with('"') {
            // Quoted field — find the closing quote, handling "" escapes.
            let mut chars = current[1..].char_indices().peekable();
            let mut field_end = None;
            while let Some((idx, ch)) = chars.next() {
                if ch == '"' {
                    if chars.peek().is_none_or(|&(_, next)| next != '"') {
                        // Closing quote (not followed by another quote).
                        field_end = Some(idx.saturating_add(1));
                        break;
                    }
                    // Escaped quote `""` — skip the next quote.
                    chars.next();
                }
            }
            if let Some(end) = field_end {
                let field = &current[1..end]; // strip opening/closing quotes
                fields.push(field);
                // Skip closing quote and comma separator.
                current = end
                    .checked_add(1)
                    .map_or("", |n| current.get(n..).unwrap_or(""));
                if current.starts_with(',') {
                    current = &current[1..];
                }
            } else {
                // Unterminated quote — treat rest as field.
                fields.push(&current[1..]);
                current = "";
            }
        } else {
            // Unquoted field — split on comma.
            match current.find(',') {
                Some(pos) => {
                    fields.push(&current[..pos]);
                    current = pos
                        .checked_add(1)
                        .map_or("", |n| current.get(n..).unwrap_or(""));
                }
                None => {
                    fields.push(current);
                    current = "";
                }
            }
        }
    }
    // If the line ended with a comma, we need an extra empty field.
    if line.ends_with(',') {
        fields.push("");
    }
    fields
}

#[cfg(test)]
mod cancellation_tests {
    use super::{QueryCancellationGuard, RangeLineReader};
    use shardline_server_core::ServerObjectStore;
    use shardline_storage::ObjectKey;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn dropped_query_guard_cancels_worker() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = QueryCancellationGuard::new(cancelled.clone());
        drop(guard);
        assert!(cancelled.load(Ordering::Relaxed));
    }

    #[test]
    fn deadline_cancellation_is_idempotent() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = QueryCancellationGuard::new(cancelled.clone());
        guard.cancel("deadline");
        guard.cancel("deadline");
        assert!(cancelled.load(Ordering::Relaxed));
    }

    #[test]
    fn cancelled_text_reader_stops_before_fetching_another_range() {
        let cancelled = Arc::new(AtomicBool::new(true));
        let mut reader = RangeLineReader::new(
            &ServerObjectStore::Blackhole,
            ObjectKey::parse("preview").unwrap(),
            1024,
            cancelled,
        );
        assert!(reader.next_line().is_err());
    }
}
