use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, Query, State},
};

use crate::{error::HubApiError, models::*};
use shardline_index::hub::HubFileEntry;
use shardline_protocol::TokenScope;

use super::{HubState, authorize_with_context, require_repository_binding};

// ---- File tree (requires Read) ----

/// Lists a repository tree at its root.
///
/// The native `huggingface_hub` client omits the trailing slash when it lists
/// every file, so this is deliberately a distinct route from the path variant.
pub(crate) async fn file_tree_at_root(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, headers, ns, repo, rev, String::new(), query).await
}

pub(crate) async fn file_tree(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo, rev, file_path)): Path<(String, String, String, String, String)>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, headers, ns, repo, rev, file_path, query).await
}

async fn file_tree_for_path(
    state: HubState,
    headers: HeaderMap,
    ns: String,
    repo: String,
    rev: String,
    file_path: String,
    query: TreeQuery,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    shardline_metrics::record_hub_api_request("file_tree", "GET", 200);
    let auth_ctx = authorize_with_context(&state, &headers, TokenScope::Read)?;
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;
    let name = format!("{ns}/{repo}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let entries = if query.recursive {
        tree_entries_recursive(&files, &file_path)
    } else {
        tree_entries_at_path(&files, &file_path)
    };
    let entries = if let Some(limit) = query.limit {
        let entries: Vec<TreeEntry> = if let Some(cursor) = &query.cursor {
            // Skip entries until we pass the cursor, then take `limit` entries.
            entries
                .into_iter()
                .skip_while(|e| &e.path != cursor)
                .skip(1) // skip the cursor entry itself
                .take(limit)
                .collect()
        } else {
            entries.into_iter().take(limit).collect()
        };
        entries
    } else {
        entries
    };
    Ok(Json(entries))
}

pub(crate) fn tree_entries_at_path(files: &[HubFileEntry], path: &str) -> Vec<TreeEntry> {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };

    let mut entries = Vec::new();
    let mut seen_dirs = std::collections::HashSet::new();

    for file in files {
        if !prefix.is_empty() && !file.path.starts_with(&prefix) {
            continue;
        }
        let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);

        if let Some((dir, _rest)) = relative.split_once('/') {
            if seen_dirs.insert(dir.to_owned()) {
                entries.push(TreeEntry {
                    entry_type: "directory".to_owned(),
                    path: dir.to_owned(),
                    size: None,
                    oid: None,
                    lfs: None,
                });
            }
        } else {
            let lfs = if file.is_lfs {
                Some(TreeEntryLfs {
                    oid: file.sha.clone(),
                    size: file.size,
                })
            } else {
                None
            };
            entries.push(TreeEntry {
                entry_type: "file".to_owned(),
                path: relative.to_owned(),
                size: Some(file.size),
                oid: Some(file.sha.clone()),
                lfs,
            });
        }
    }

    entries.sort_by(|a, b| {
        a.entry_type
            .cmp(&b.entry_type)
            .then_with(|| a.path.cmp(&b.path))
    });

    entries
}

/// Lists all files recursively under a given path prefix.
pub(crate) fn tree_entries_recursive(files: &[HubFileEntry], path: &str) -> Vec<TreeEntry> {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };

    let mut entries = Vec::new();

    for file in files {
        if !prefix.is_empty() && !file.path.starts_with(&prefix) {
            continue;
        }
        let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);
        if relative.is_empty() {
            continue;
        }

        let lfs = if file.is_lfs {
            Some(TreeEntryLfs {
                oid: file.sha.clone(),
                size: file.size,
            })
        } else {
            None
        };
        entries.push(TreeEntry {
            entry_type: "file".to_owned(),
            path: relative.to_owned(),
            size: Some(file.size),
            oid: Some(file.sha.clone()),
            lfs,
        });
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}
