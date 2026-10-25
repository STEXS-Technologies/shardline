#[cfg(test)]
use axum::{
    Json,
    extract::{Path, Query, State},
};

use crate::{error::HubApiError, models::*};
use shardline_index::hub::{HubFileEntry, HubTreePageEntry, HubTreePageOptions};

use super::{HubRepository, HubState};

// ---- File tree (requires Read) ----

/// Lists a repository tree at its root.
///
/// The native `huggingface_hub` client omits the trailing slash when it lists
/// every file, so this is deliberately a distinct route from the path variant.
#[cfg(test)]
pub(crate) async fn file_tree_at_root(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_repo_type, ns, repo_name, rev)): Path<(String, String, String, String)>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, _repo, ns, repo_name, rev, String::new(), query)
        .await
        .map(|listing| Json(listing.entries))
}

#[cfg(test)]
pub(crate) async fn file_tree(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_repo_type, ns, repo_name, rev, file_path)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, _repo, ns, repo_name, rev, file_path, query)
        .await
        .map(|listing| Json(listing.entries))
}

pub(super) struct TreeListing {
    pub entries: Vec<TreeEntry>,
    pub revision: String,
    pub has_more: bool,
}

pub(super) fn normalize_root_path(path: String) -> String {
    if path == "." { String::new() } else { path }
}

pub(super) async fn file_tree_for_path(
    state: HubState,
    _repo: HubRepository,
    ns: String,
    repo_name: String,
    rev: String,
    file_path: String,
    query: TreeQuery,
) -> Result<TreeListing, HubApiError> {
    let file_path = normalize_root_path(file_path);
    shardline_metrics::record_hub_api_request("file_tree", "GET", 200);
    // The extractor has already authorized this request and minted `repo`.
    let name = format!("{ns}/{repo_name}");
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(&state, &name, &rev, _repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(&state, &name, &rev, &commit_sha, _repo.capability())
        .await?;
    if let Some(limit) = query.limit {
        // Store cursors and entries are relative to the requested directory;
        // the Hub wire contract always uses repository-relative paths.
        let prefix = if file_path.is_empty() {
            String::new()
        } else {
            format!("{file_path}/")
        };
        let page = state
            .store
            .get_tree_page(
                &commit_sha,
                &HubTreePageOptions {
                    path: file_path.clone(),
                    recursive: query.recursive,
                    cursor: query
                        .cursor
                        .map(|cursor| cursor.strip_prefix(&prefix).unwrap_or(&cursor).to_owned()),
                    limit,
                },
            )
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        let has_more = page.has_more;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| match entry {
                HubTreePageEntry::Directory { path } => TreeEntry {
                    entry_type: "directory".to_owned(),
                    path: format!("{prefix}{path}"),
                    size: None,
                    oid: None,
                    lfs: None,
                },
                HubTreePageEntry::File(file) => TreeEntry {
                    entry_type: "file".to_owned(),
                    path: format!("{prefix}{}", file.path),
                    size: Some(file.size),
                    oid: Some(file.sha.clone()),
                    lfs: file.is_lfs.then_some(TreeEntryLfs {
                        oid: file.sha.clone(),
                        size: file.size,
                        pointer_size: lfs_pointer_size(&file.sha, file.size),
                    }),
                },
            })
            .collect();
        return Ok(TreeListing {
            entries: with_git_identities(
                &state,
                &name,
                &commit_sha,
                _repo.capability(),
                &file_path,
                entries,
            )
            .await?,
            revision: commit_sha,
            has_more,
        });
    }
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let entries = if query.recursive {
        tree_entries_recursive(&files, &file_path)?
    } else {
        tree_entries_at_path(&files, &file_path)
    };
    Ok(TreeListing {
        entries: with_git_identities(
            &state,
            &name,
            &commit_sha,
            _repo.capability(),
            &file_path,
            entries,
        )
        .await?,
        revision: commit_sha,
        has_more: false,
    })
}

async fn with_git_identities(
    state: &HubState,
    repo: &str,
    revision: &str,
    auth: &shardline_server_core::AuthorizedRepository,
    requested_path: &str,
    mut entries: Vec<TreeEntry>,
) -> Result<Vec<TreeEntry>, HubApiError> {
    if entries.is_empty() && requested_path.is_empty() {
        return Ok(entries);
    }
    crate::git::smart_http::public_commit_id(state, repo, revision, auth).await?;
    let mut paths: Vec<_> = entries.iter().map(|entry| entry.path.clone()).collect();
    if !requested_path.is_empty() {
        paths.push(requested_path.to_owned());
    }
    let identities = crate::git::smart_http::tree_oids(
        state.clone(),
        repo.to_owned(),
        revision.to_owned(),
        paths,
        auth.clone(),
    )
    .await?;
    if !requested_path.is_empty()
        && !identities
            .get(requested_path)
            .is_some_and(|entry| matches!(entry.mode.as_str(), "40000" | "040000"))
    {
        return Err(HubApiError::NotFound);
    }
    for entry in &mut entries {
        entry.oid = Some(
            identities
                .get(&entry.path)
                .ok_or_else(|| {
                    HubApiError::CasError(
                        "listed path is missing from verified Git tree".to_owned(),
                    )
                })?
                .oid
                .clone(),
        );
    }
    Ok(entries)
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
                    path: format!("{prefix}{dir}"),
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
                    pointer_size: lfs_pointer_size(&file.sha, file.size),
                })
            } else {
                None
            };
            entries.push(TreeEntry {
                entry_type: "file".to_owned(),
                path: file.path.clone(),
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

/// Lists files and directories recursively under a given path prefix.
pub(crate) fn tree_entries_recursive(
    files: &[HubFileEntry],
    path: &str,
) -> Result<Vec<TreeEntry>, HubApiError> {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };

    let mut entries = Vec::new();
    let mut directories = std::collections::BTreeSet::new();

    for file in files {
        if !prefix.is_empty() && !file.path.starts_with(&prefix) {
            continue;
        }
        let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);
        if relative.is_empty() {
            continue;
        }
        for (index, _) in relative.match_indices('/') {
            directories.insert(format!("{prefix}{}", &relative[..index]));
            if directories.len() > shardline_index::hub::HUB_TREE_READ_CEILING {
                return Err(HubApiError::CasError(
                    "recursive directory metadata exceeds tree limit".to_owned(),
                ));
            }
        }

        let lfs = if file.is_lfs {
            Some(TreeEntryLfs {
                oid: file.sha.clone(),
                size: file.size,
                pointer_size: lfs_pointer_size(&file.sha, file.size),
            })
        } else {
            None
        };
        entries.push(TreeEntry {
            entry_type: "file".to_owned(),
            path: file.path.clone(),
            size: Some(file.size),
            oid: Some(file.sha.clone()),
            lfs,
        });
    }

    entries.extend(directories.into_iter().map(|directory| TreeEntry {
        entry_type: "directory".to_owned(),
        path: directory,
        size: None,
        oid: None,
        lfs: None,
    }));
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

fn lfs_pointer_size(oid: &str, size: u64) -> u64 {
    format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n").len()
        as u64
}
