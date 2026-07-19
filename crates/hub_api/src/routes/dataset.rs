use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, Query, State},
};

use crate::{error::HubApiError, models::*};
use shardline_index::hub::{HubFileEntry, HubRepoType};
use shardline_protocol::TokenScope;

use super::HubState;
use super::authorize;

// ---- Dataset viewer endpoints ----

/// Lists parquet/data files in a dataset repository.
pub(crate) async fn dataset_parquet(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
) -> Result<Json<DatasetParquetResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_parquet", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
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
    headers: HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
    Query(query): Query<DatasetFirstRowsQuery>,
) -> Result<Json<DatasetFirstRowsResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_first_rows", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
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
    let content = data_file.inline_content.as_deref().ok_or_else(|| {
        HubApiError::PathValidation("file content not available inline".to_owned())
    })?;
    let limit = query.limit.min(1000);
    let rows = parse_rows_from_content(content, &data_file.path, 0, limit)?;
    let columns = rows
        .first()
        .map(|r| r.columns.keys().cloned().collect())
        .unwrap_or_default();
    Ok(Json(DatasetFirstRowsResponse { columns, rows }))
}

/// Returns rows from a dataset split with pagination.
pub(crate) async fn dataset_viewer(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((ns, repo, split)): Path<(String, String, String)>,
    Query(query): Query<DatasetViewerQuery>,
) -> Result<Json<DatasetViewerResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_viewer", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
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
    let content = data_file.inline_content.as_deref().ok_or_else(|| {
        HubApiError::PathValidation("file content not available inline".to_owned())
    })?;
    let length = query.length.min(10000);
    let rows = parse_rows_from_content(content, &data_file.path, query.offset, length)?;
    let columns = rows
        .first()
        .map(|r| r.columns.keys().cloned().collect())
        .unwrap_or_default();
    Ok(Json(DatasetViewerResponse {
        columns,
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
