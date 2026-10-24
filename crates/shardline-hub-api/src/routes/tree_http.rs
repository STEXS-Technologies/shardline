//! Expanded SDK tree metadata without changing the public Rust model shapes.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, header::LINK},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use super::{HubRepository, HubState, tree};
use crate::{
    error::HubApiError,
    models::{TreeEntry, TreeQuery},
};

const EXPANDED_PAGE_SIZE: usize = 50;

#[derive(Serialize)]
struct ExpandedTreeEntry {
    #[serde(flatten)]
    entry: TreeEntry,
    #[serde(rename = "lastCommit")]
    last_commit: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct TreeHttpQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    expand: bool,
}

impl TreeHttpQuery {
    fn into_tree(self) -> TreeQuery {
        TreeQuery {
            limit: self.limit,
            cursor: self.cursor,
            recursive: self.recursive,
        }
    }
}

pub(crate) async fn tree_root_http(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((kind, ns, name, revision)): Path<(String, String, String, String)>,
    Query(query): Query<TreeHttpQuery>,
) -> Result<Response, HubApiError> {
    if !query.expand {
        return tree::file_tree_at_root(
            State(state),
            repo,
            Path((kind, ns, name, revision)),
            Query(query.into_tree()),
        )
        .await
        .map(IntoResponse::into_response);
    }
    expanded_tree(
        state,
        repo,
        (kind, ns, name, revision),
        String::new(),
        query.into_tree(),
    )
    .await
}

pub(crate) async fn tree_path_http(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((kind, ns, name, revision, path)): Path<(String, String, String, String, String)>,
    Query(query): Query<TreeHttpQuery>,
) -> Result<Response, HubApiError> {
    let path = tree::normalize_root_path(path);
    if !query.expand {
        return tree::file_tree(
            State(state),
            repo,
            Path((kind, ns, name, revision, path)),
            Query(query.into_tree()),
        )
        .await
        .map(IntoResponse::into_response);
    }
    expanded_tree(
        state,
        repo,
        (kind, ns, name, revision),
        path,
        query.into_tree(),
    )
    .await
}

async fn expanded_tree(
    state: HubState,
    repo: HubRepository,
    (kind, ns, name, revision): (String, String, String, String),
    path: String,
    mut query: TreeQuery,
) -> Result<Response, HubApiError> {
    query.limit = Some(
        query
            .limit
            .unwrap_or(EXPANDED_PAGE_SIZE)
            .min(EXPANDED_PAGE_SIZE),
    );
    let recursive = query.recursive;
    let limit = query.limit.unwrap_or(EXPANDED_PAGE_SIZE);
    let auth = repo.capability().clone();
    let repo_id = format!("{ns}/{name}");
    let listing = tree::file_tree_for_path(
        state.clone(),
        repo,
        ns.clone(),
        name.clone(),
        revision,
        path.clone(),
        query,
    )
    .await?;
    if listing.entries.is_empty() {
        return Ok(Json(Vec::<ExpandedTreeEntry>::new()).into_response());
    }
    let files = state
        .store
        .get_files(&listing.revision)
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let paths: Vec<_> = listing
        .entries
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    let mut last = super::path_history::last_path_commits(
        &state,
        &repo_id,
        &listing.revision,
        &files,
        &paths,
        &auth,
    )
    .await?;
    let mut headers = HeaderMap::new();
    if listing.has_more
        && let Some(cursor) = paths.last()
    {
        let public =
            crate::git::smart_http::public_commit_id(&state, &repo_id, &listing.revision, &auth)
                .await?;
        let mut url = url::Url::parse(&state.public_base_url).map_err(|_error| {
            HubApiError::CasError("invalid configured public Hub URL".to_owned())
        })?;
        url.set_query(None);
        url.set_fragment(None);
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                HubApiError::CasError("public Hub URL cannot contain tree paths".to_owned())
            })?;
            segments
                .pop_if_empty()
                .extend(["api", &kind, &ns, &name, "tree", &public]);
            if !path.is_empty() {
                segments.extend(path.split('/'));
            }
        }
        url.query_pairs_mut()
            .append_pair("expand", "true")
            .append_pair("recursive", &recursive.to_string())
            .append_pair("limit", &limit.to_string())
            .append_pair("cursor", cursor);
        headers.insert(
            LINK,
            HeaderValue::from_str(&format!("<{url}>; rel=\"next\"")).map_err(|_error| {
                HubApiError::CasError("invalid tree pagination URL".to_owned())
            })?,
        );
    }
    let entries: Vec<_> = listing
        .entries
        .into_iter()
        .map(|entry| ExpandedTreeEntry {
            last_commit: last.remove(&entry.path),
            entry,
        })
        .collect();
    Ok((headers, Json(entries)).into_response())
}
