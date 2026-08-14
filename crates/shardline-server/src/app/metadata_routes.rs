//! M5a server-side metadata endpoints: path -> `file_id` tree, revision registry.
//!
//! These routes live under `/api/{provider}/{owner}/{repo}/...`. Read endpoints are
//! read-token authenticated; registration, deregistration, and revision mutations are
//! write-token authenticated. When authentication is configured, the token claims must
//! match the route scope.
//!
//! # Listing pagination note
//!
//! The listing endpoint paginates on the **raw** registered path (keyset). A derived
//! directory whose contributing raw paths straddle a page boundary may be emitted on
//! more than one page; clients deduplicate by `entries[].path`.

use std::{collections::HashSet, sync::Arc};

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use shardline_index::{RepoKey, RevisionRecord, TreeKey};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server_core::AuthorizedRepository;

use crate::{
    ServerError,
    app::{
        AppState, endpoint_body_limit,
        reconstruction_routes::{XetRepository, XetWriteRepository},
    },
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

/// Maximum length of a canonical path in bytes.
pub const MAX_PATH_BYTES: usize = 4096;
/// Default listing page size.
pub const DEFAULT_TREE_LIST_LIMIT: usize = 1000;
/// Maximum accepted listing page size.
pub const MAX_TREE_LIST_LIMIT: usize = 10_000;
/// Maximum accepted registration request body in bytes.
pub const MAX_REGISTER_BODY_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
pub(super) struct TreeLookupQuery {
    #[serde(rename = "path")]
    path: Option<String>,
    #[serde(rename = "prefix")]
    prefix: Option<String>,
    #[serde(rename = "limit")]
    limit: Option<usize>,
    #[serde(rename = "cursor")]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DeletePathQuery {
    #[serde(rename = "recursive")]
    recursive: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RegisterPathBody {
    #[serde(rename = "fileId")]
    file_id: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ResolveResponse {
    path: String,
    #[serde(rename = "fileId")]
    file_id: String,
    size: u64,
    #[serde(rename = "updatedAt")]
    updated_at: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct ListEntry {
    path: String,
    #[serde(rename = "isDir")]
    is_dir: bool,
    #[serde(rename = "fileId")]
    file_id: Option<String>,
    size: Option<u64>,
    #[serde(rename = "updatedAt")]
    updated_at: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct ListResponse {
    entries: Vec<ListEntry>,
    #[serde(rename = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct RegisterResponse {
    path: String,
    #[serde(rename = "fileId")]
    file_id: String,
    size: u64,
    #[serde(rename = "updatedAt")]
    updated_at: u64,
    created: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct RevisionJson {
    name: String,
    #[serde(rename = "createdAt")]
    created_at: u64,
    #[serde(rename = "updatedAt")]
    updated_at: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct RevisionsResponse {
    revisions: Vec<RevisionJson>,
}

#[derive(Debug, Serialize)]
pub(super) struct DeletePathResponse {
    path: String,
    deleted: u64,
    recursive: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct DeleteRevisionResponse {
    name: String,
    deleted: bool,
}

/// Normalizes a canonical path or prefix.
///
/// When `is_prefix` is true, a single trailing slash is preserved as the boundary
/// marker and an empty input is accepted as the repository root.
fn normalize_path(input: &str, is_prefix: bool) -> Result<String, ServerError> {
    // URL-decode once. Axum already decodes path params and query strings, so this
    // is effectively a no-op for well-formed input but protects wildcard captures.
    let decoded = percent_encoding::percent_decode_str(input)
        .decode_utf8()
        .map_err(|_error| ServerError::InvalidPath)?;
    let decoded = decoded.as_ref();
    if decoded.len() > MAX_PATH_BYTES {
        return Err(ServerError::InvalidPath);
    }
    if decoded.chars().any(char::is_control) || decoded.contains('\\') {
        return Err(ServerError::InvalidPath);
    }
    if decoded.starts_with('/') {
        return Err(ServerError::InvalidPath);
    }
    if is_prefix && decoded.is_empty() {
        return Ok(String::new());
    }
    // Strip exactly one trailing slash for a prefix boundary marker so a client
    // may send `data/`; a double slash still yields an empty segment and is rejected.
    let body = if is_prefix {
        decoded.strip_suffix('/').unwrap_or(decoded)
    } else {
        decoded
    };
    if body.is_empty() {
        return Err(ServerError::InvalidPath);
    }
    for segment in body.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(ServerError::InvalidPath);
        }
    }
    if is_prefix {
        Ok(format!("{body}/"))
    } else {
        Ok(body.to_owned())
    }
}

/// Validates a revision name for path/body endpoints.
fn validate_revision(rev: &str) -> Result<(), ServerError> {
    if rev.is_empty() || rev.len() > 512 || rev.chars().any(char::is_control) {
        return Err(ServerError::InvalidPath);
    }
    Ok(())
}

/// Cross-checks the authenticated scope against the full route scope, including revision.
fn check_scope(
    auth: Option<&AuthorizedRepository>,
    provider: &str,
    owner: &str,
    repo: &str,
    rev: &str,
) -> Result<(), ServerError> {
    if let Some(repository_scope) = auth.and_then(AuthorizedRepository::repository)
        && (repository_scope.provider().as_str() != provider
            || repository_scope.owner() != owner
            || repository_scope.name() != repo
            || repository_scope.revision() != Some(rev))
    {
        return Err(ServerError::InsufficientScope);
    }
    Ok(())
}

/// Cross-checks the authenticated scope against the route repository identity only.
fn check_scope_repo(
    auth: Option<&AuthorizedRepository>,
    provider: &str,
    owner: &str,
    repo: &str,
) -> Result<(), ServerError> {
    if let Some(repository_scope) = auth.and_then(AuthorizedRepository::repository)
        && (repository_scope.provider().as_str() != provider
            || repository_scope.owner() != owner
            || repository_scope.name() != repo)
    {
        return Err(ServerError::InsufficientScope);
    }
    Ok(())
}

fn parse_limit(limit: Option<usize>) -> Result<usize, ServerError> {
    match limit {
        None => Ok(DEFAULT_TREE_LIST_LIMIT),
        Some(value) if (1..=MAX_TREE_LIST_LIMIT).contains(&value) => Ok(value),
        Some(_) => Err(ServerError::InvalidPath),
    }
}

/// Strips the boundary trailing slash from a canonical prefix to derive the scan prefix.
#[must_use]
fn scan_prefix(prefix: &str) -> &str {
    prefix.strip_suffix('/').unwrap_or(prefix)
}

/// Derives the single immediate child for a raw registered path under a scan prefix.
///
/// Returns `(child_path, is_dir)`.
fn derive_child(scan_prefix: &str, raw_path: &str) -> Option<(String, bool)> {
    if scan_prefix.is_empty() {
        return Some(raw_path.split_once('/').map_or_else(
            || (raw_path.to_owned(), false),
            |(first, _rest)| (format!("{first}/"), true),
        ));
    }
    if raw_path == scan_prefix {
        return Some((raw_path.to_owned(), false));
    }
    let rest = raw_path.strip_prefix(&format!("{scan_prefix}/"))?;
    Some(rest.split_once('/').map_or_else(
        || (raw_path.to_owned(), false),
        |(first, _rest)| (format!("{scan_prefix}/{first}/"), true),
    ))
}

/// Handles both §1.1 (path resolve) and §1.2 (listing) query modes on the tree route.
pub(super) async fn tree_lookup(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev)): Path<(String, String, String, String)>,
    repo_capability: XetRepository,
    Query(query): Query<TreeLookupQuery>,
) -> Result<Response, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope(auth, &provider, &owner, &repo, &rev)?;
    validate_revision(&rev)?;

    if let Some(path) = query.path.as_deref() {
        let path = normalize_path(path, false)?;
        let key = TreeKey::new(&provider, &owner, &repo, &rev);
        let entry = state
            .backend
            .resolve_tree_path(&key, &path)
            .await?
            .ok_or(ServerError::NotFound)?;
        return Ok(Json(ResolveResponse {
            path: entry.path,
            file_id: entry.file_id,
            size: entry.size_bytes,
            updated_at: entry.updated_at_unix_seconds,
        })
        .into_response());
    }

    let prefix = match query.prefix.as_deref() {
        Some(prefix) => normalize_path(prefix, true)?,
        None => String::new(),
    };
    let limit = parse_limit(query.limit)?;
    let key = TreeKey::new(&provider, &owner, &repo, &rev);
    let response =
        build_list_response(&state, &key, &prefix, query.cursor.as_deref(), limit).await?;
    Ok(Json(response).into_response())
}

async fn build_list_response(
    state: &Arc<AppState>,
    key: &TreeKey,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<ListResponse, ServerError> {
    let scan_limit = limit.saturating_mul(4);
    let prefix = scan_prefix(prefix).to_owned();
    let raw = state
        .backend
        .scan_tree_raw(key, &prefix, cursor, scan_limit)
        .await?;

    let mut entries: Vec<ListEntry> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut last_raw: Option<String> = None;
    let mut emitted = 0usize;

    for row in &raw {
        last_raw = Some(row.path.clone());
        let Some((child_path, is_dir)) = derive_child(&prefix, &row.path) else {
            continue;
        };
        if !seen.insert(child_path.clone()) {
            continue;
        }
        if is_dir {
            entries.push(ListEntry {
                path: child_path,
                is_dir: true,
                file_id: None,
                size: None,
                updated_at: None,
            });
        } else {
            entries.push(ListEntry {
                path: child_path,
                is_dir: false,
                file_id: Some(row.file_id.clone()),
                size: Some(row.size_bytes),
                updated_at: Some(row.updated_at_unix_seconds),
            });
        }
        emitted = emitted.saturating_add(1);
        if emitted >= limit {
            break;
        }
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));

    // A page is not exhausted when the emit limit was reached or the scan returned a
    // full batch (more raw rows may exist beyond the batch).
    let more = emitted >= limit || raw.len() >= scan_limit;
    let next_cursor = if more { last_raw } else { None };
    Ok(ListResponse {
        entries,
        next_cursor,
    })
}

pub(super) async fn register_path(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev, path)): Path<(String, String, String, String, String)>,
    repo_capability: XetWriteRepository,
    body: Body,
) -> Result<Json<RegisterResponse>, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope(auth, &provider, &owner, &repo, &rev)?;
    validate_revision(&rev)?;
    let path = normalize_path(&path, false)?;

    let max_bytes = endpoint_body_limit(
        state.config.max_request_body_bytes(),
        MAX_REGISTER_BODY_BYTES,
    )?;
    let mut reader = RequestBodyReader::from_body(body, max_bytes)?;
    let bytes = read_body_to_bytes(&mut reader).await?;
    let parsed: RegisterPathBody =
        serde_json::from_slice(&bytes).map_err(|_error| ServerError::InvalidPath)?;

    let key = TreeKey::new(&provider, &owner, &repo, &rev);
    let scope = repo_capability.capability().namespace();
    let outcome = state
        .backend
        .register_tree_path(&key, &path, &parsed.file_id, scope)
        .await?;
    Ok(Json(RegisterResponse {
        path: outcome.entry.path,
        file_id: outcome.entry.file_id,
        size: outcome.entry.size_bytes,
        updated_at: outcome.entry.updated_at_unix_seconds,
        created: outcome.created,
    }))
}

pub(super) async fn delete_path(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev, path)): Path<(String, String, String, String, String)>,
    repo_capability: XetWriteRepository,
    Query(query): Query<DeletePathQuery>,
) -> Result<Json<DeletePathResponse>, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope(auth, &provider, &owner, &repo, &rev)?;
    validate_revision(&rev)?;
    let path = normalize_path(&path, false)?;
    let recursive = query.recursive.unwrap_or(false);
    let key = TreeKey::new(&provider, &owner, &repo, &rev);
    let deleted = state
        .backend
        .delete_tree_path(&key, &path, recursive)
        .await?;
    Ok(Json(DeletePathResponse {
        path,
        deleted,
        recursive,
    }))
}

pub(super) async fn list_revisions(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo)): Path<(String, String, String)>,
    repo_capability: XetRepository,
) -> Result<Json<RevisionsResponse>, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope_repo(auth, &provider, &owner, &repo)?;
    let key = RepoKey::new(&provider, &owner, &repo);
    let revisions = state.backend.list_revisions(&key).await?;
    Ok(Json(RevisionsResponse {
        revisions: revisions
            .into_iter()
            .map(|record| RevisionJson {
                name: record.revision,
                created_at: record.created_at_unix_seconds,
                updated_at: record.updated_at_unix_seconds,
            })
            .collect(),
    }))
}

pub(super) async fn create_revision(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev)): Path<(String, String, String, String)>,
    repo_capability: XetWriteRepository,
) -> Result<Json<RevisionJson>, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope(auth, &provider, &owner, &repo, &rev)?;
    validate_revision(&rev)?;
    let now = unix_now_seconds_lossy();
    let record = RevisionRecord {
        provider: provider.clone(),
        owner: owner.clone(),
        repo: repo.clone(),
        revision: rev.clone(),
        created_at_unix_seconds: now,
        updated_at_unix_seconds: now,
    };
    let created = state.backend.create_revision(&record).await?;
    if !created {
        return Err(ServerError::RevisionConflict);
    }
    Ok(Json(RevisionJson {
        name: rev,
        created_at: now,
        updated_at: now,
    }))
}

pub(super) async fn delete_revision(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev)): Path<(String, String, String, String)>,
    repo_capability: XetWriteRepository,
) -> Result<Json<DeleteRevisionResponse>, ServerError> {
    let auth = Some(repo_capability.capability());
    check_scope(auth, &provider, &owner, &repo, &rev)?;
    validate_revision(&rev)?;
    let key = RepoKey::new(&provider, &owner, &repo);
    let deleted = state.backend.delete_revision(&key, &rev).await?;
    Ok(Json(DeleteRevisionResponse { name: rev, deleted }))
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_TREE_LIST_LIMIT, MAX_PATH_BYTES, MAX_TREE_LIST_LIMIT, derive_child, normalize_path,
        parse_limit, scan_prefix,
    };

    #[test]
    fn normalize_path_accepts_valid_paths() {
        assert_eq!(
            normalize_path("data/model.pt", false).unwrap(),
            "data/model.pt"
        );
        assert_eq!(normalize_path("a", false).unwrap(), "a");
        assert_eq!(normalize_path("a/b/c", false).unwrap(), "a/b/c");
    }

    #[test]
    fn normalize_path_rejects_control_and_backslash() {
        assert!(normalize_path("a\nb", false).is_err());
        assert!(normalize_path("a\tb", false).is_err());
        assert!(normalize_path("a\\b", false).is_err());
    }

    #[test]
    fn normalize_path_rejects_dot_and_dotdot_segments() {
        assert!(normalize_path(".", false).is_err());
        assert!(normalize_path("..", false).is_err());
        assert!(normalize_path("a/../b", false).is_err());
        assert!(normalize_path("../b", false).is_err());
        assert!(normalize_path("a/./b", false).is_err());
    }

    #[test]
    fn normalize_path_rejects_leading_slash_and_empty_segments() {
        assert!(normalize_path("/a", false).is_err());
        assert!(normalize_path("a//b", false).is_err());
        assert!(normalize_path("a/b/", false).is_err());
        assert!(normalize_path("", false).is_err());
    }

    #[test]
    fn normalize_path_rejects_overlong() {
        let long = "a".repeat(MAX_PATH_BYTES + 1);
        assert!(normalize_path(&long, false).is_err());
        let max = "a".repeat(MAX_PATH_BYTES);
        assert!(normalize_path(&max, false).is_ok());
    }

    #[test]
    fn normalize_path_handles_prefix_boundary_marker() {
        assert_eq!(normalize_path("", true).unwrap(), "");
        assert_eq!(normalize_path("data/", true).unwrap(), "data/");
        assert_eq!(normalize_path("data", true).unwrap(), "data/");
        assert!(normalize_path("data//", true).is_err());
    }

    #[test]
    fn normalize_path_percent_decodes_once() {
        assert_eq!(normalize_path("a%20b.txt", false).unwrap(), "a b.txt");
    }

    #[test]
    fn parse_limit_applies_defaults_and_bounds() {
        assert_eq!(parse_limit(None).unwrap(), DEFAULT_TREE_LIST_LIMIT);
        assert_eq!(parse_limit(Some(1)).unwrap(), 1);
        assert_eq!(
            parse_limit(Some(MAX_TREE_LIST_LIMIT)).unwrap(),
            MAX_TREE_LIST_LIMIT
        );
        assert!(parse_limit(Some(0)).is_err());
        assert!(parse_limit(Some(MAX_TREE_LIST_LIMIT + 1)).is_err());
    }

    #[test]
    fn scan_prefix_strips_boundary_slash() {
        assert_eq!(scan_prefix("data/"), "data");
        assert_eq!(scan_prefix(""), "");
    }

    #[test]
    fn derive_child_at_root_maps_files_and_dirs() {
        assert_eq!(
            derive_child("", "readme.md"),
            Some(("readme.md".to_owned(), false))
        );
        assert_eq!(derive_child("", "a/b/c.txt"), Some(("a/".to_owned(), true)));
    }

    #[test]
    fn derive_child_under_prefix_is_one_level() {
        assert_eq!(
            derive_child("data", "data/model.pt"),
            Some(("data/model.pt".to_owned(), false))
        );
        assert_eq!(
            derive_child("data", "data/sub/x.txt"),
            Some(("data/sub/".to_owned(), true))
        );
        assert_eq!(derive_child("data", "other.txt"), None);
        assert_eq!(
            derive_child("data", "data"),
            Some(("data".to_owned(), false))
        );
    }
}
