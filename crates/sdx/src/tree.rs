//! Path-namespace client (M5b, `docs/SDX_PLAN.md` §4.3): map paths to content
//! `file_id`s and back, against the M5a server metadata endpoints.
//!
//! Exposes [`XetClient::resolve_path`], [`XetClient::list_dir`] /
//! [`XetClient::list_dir_paged`] / [`XetClient::list_dir_all`],
//! [`XetClient::register_path`], and [`XetClient::delete_path`].
//!
//! Requests are issued through the M4 [`RetryContext`] (read token for
//! resolves/lists, write token for registrations/deregistrations), with 401/403
//! token refresh and jittered backoff. Route paths are the `XET_TREE_ROUTE` /
//! `XET_PATH_ROUTE` templates from `shardline_xet_adapter`, substituted with the
//! client's provider/owner/repo/revision identity.
//!
//! # Listing dedup contract
//!
//! The server paginates on the raw registered path (keyset). A derived
//! directory whose contributing raw paths straddle a page boundary may be
//! emitted on more than one page, so [`XetClient::list_dir_all`] deduplicates by
//! `entries[].path`.

use std::collections::HashSet;

use reqwest::Method;
use serde::Deserialize;
use shardline_xet_adapter::{XET_PATH_ROUTE, XET_TREE_ROUTE};

use crate::{
    auth::{RepositoryId, TokenService},
    client::XetClient,
    error::SdxError,
    retry::{RetryContext, RetryMarkers, RetryPolicy, RetryScope},
    session::DownloadSessionInner,
    transfer::TransferClient,
};

/// A resolved path → `file_id` mapping (`{path,fileId,size,updatedAt}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    /// Canonical path (no leading/trailing slash).
    pub path: String,
    /// Content-derived file identifier (64 lowercase hex).
    pub file_id: String,
    /// Registered file size in bytes.
    pub size: u64,
    /// Last registration time as Unix seconds.
    pub updated_at: u64,
}

/// A single directory-listing entry (file or derived directory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// Canonical child path; directories carry a trailing slash.
    pub path: String,
    /// Whether this is a derived directory (children aggregated) or a file.
    pub is_dir: bool,
    /// File id, present only for file entries.
    pub file_id: Option<String>,
    /// File size in bytes, present only for file entries.
    pub size: Option<u64>,
    /// Last registration time, present only for file entries.
    pub updated_at: Option<u64>,
}

/// One page of a directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirListing {
    /// Entries for this page (already sorted by path).
    pub entries: Vec<DirEntry>,
    /// Opaque keyset cursor for the next page, `None` when exhausted.
    pub next_cursor: Option<String>,
}

/// Result of registering a path (mirrors the server's `RegisterResponse`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterResult {
    /// The registered path entry.
    pub entry: PathEntry,
    /// Whether this path was newly created (`false` on a re-registration that
    /// repointed an existing path).
    pub created: bool,
}

// ── wire response shapes (camelCase) ────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolveResponse {
    path: String,
    file_id: String,
    size: u64,
    updated_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListEntry {
    path: String,
    is_dir: bool,
    file_id: Option<String>,
    size: Option<u64>,
    updated_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    entries: Vec<ListEntry>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterResponse {
    path: String,
    file_id: String,
    size: u64,
    updated_at: u64,
    created: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeletePathResponse {
    deleted: u64,
}

/// Low-level transport for the metadata endpoints. Both the [`XetClient`]
/// surface and the upload session's automatic registration use it.
#[derive(Clone)]
pub(crate) struct MetadataClient {
    pub(crate) transfer: TransferClient,
    pub(crate) tokens: TokenService,
    pub(crate) api_base: String,
    pub(crate) repository: RepositoryId,
    pub(crate) retry_policy: RetryPolicy,
}

impl MetadataClient {
    /// Builds a metadata client from the client's shared session state.
    pub(crate) fn from_download(inner: &DownloadSessionInner) -> Self {
        Self {
            transfer: inner.transfer.clone(),
            tokens: inner.tokens.clone(),
            api_base: inner.api_base.clone(),
            repository: inner.repository.clone(),
            retry_policy: inner.retry_policy.clone(),
        }
    }

    /// Builds a metadata client from the upload session's shared state.
    pub(crate) fn from_upload(
        transfer: &TransferClient,
        tokens: &TokenService,
        api_base: &str,
        repository: &RepositoryId,
        retry_policy: &RetryPolicy,
    ) -> Self {
        Self {
            transfer: transfer.clone(),
            tokens: tokens.clone(),
            api_base: api_base.to_owned(),
            repository: repository.clone(),
            retry_policy: retry_policy.clone(),
        }
    }

    pub(crate) fn read_retry(&self) -> RetryContext {
        RetryContext {
            policy: self.retry_policy.clone(),
            tokens: Some(self.tokens.clone()),
            scope: RetryScope::Read,
            markers: RetryMarkers {
                retry_on_403: true,
                ..RetryMarkers::default()
            },
        }
    }

    pub(crate) fn write_retry(&self) -> RetryContext {
        RetryContext {
            policy: self.retry_policy.clone(),
            tokens: Some(self.tokens.clone()),
            scope: RetryScope::Write,
            markers: RetryMarkers {
                retry_on_403: true,
                ..RetryMarkers::default()
            },
        }
    }

    /// Substitutes `{provider}/{owner}/{repo}/{rev}` into a route template.
    pub(crate) fn repo_route(&self, template: &str) -> String {
        template
            .replace("{provider}", &self.repository.provider)
            .replace("{owner}", &self.repository.owner)
            .replace("{repo}", &self.repository.repo)
            .replace("{rev}", &self.repository.revision)
    }

    /// Substitutes only the repo-scope placeholders (`{provider}/{owner}/{repo}`),
    /// leaving `{rev}` for a per-call revision argument (used by the revision
    /// create/delete routes, whose revision is not the client's default).
    pub(crate) fn repo_route_scope(&self, template: &str) -> String {
        template
            .replace("{provider}", &self.repository.provider)
            .replace("{owner}", &self.repository.owner)
            .replace("{repo}", &self.repository.repo)
    }

    /// Issues a request through the retry context and returns the raw body.
    pub(crate) async fn send(
        &self,
        retry: &RetryContext,
        token: String,
        method: Method,
        url: String,
        body: Option<serde_json::Value>,
    ) -> Result<Vec<u8>, SdxError> {
        let transfer = self.transfer.clone();
        let bytes = retry
            .run(token, move |tok| {
                let transfer = transfer.clone();
                let url = url.clone();
                let method = method.clone();
                let body = body.clone();
                async move {
                    let (_status, body) = transfer
                        .request_raw(&method, &url, &tok, body.as_ref())
                        .await?;
                    Ok(body)
                }
            })
            .await?;
        Ok(bytes)
    }

    async fn resolve_path(&self, path: &str) -> Result<PathEntry, SdxError> {
        let retry = self.read_retry();
        let token = self.tokens.read_token().await?;
        let route = self.repo_route(XET_TREE_ROUTE);
        let url = build_url(&self.api_base, &route, &[("path", path)]);
        let body = self
            .send(&retry, token.token, Method::GET, url, None)
            .await?;
        let response: ResolveResponse = serde_json::from_slice(&body)
            .map_err(|error| metadata_parse("resolve_path", &error))?;
        Ok(PathEntry {
            path: response.path,
            file_id: response.file_id,
            size: response.size,
            updated_at: response.updated_at,
        })
    }

    async fn list_paged(
        &self,
        prefix: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<DirListing, SdxError> {
        let retry = self.read_retry();
        let token = self.tokens.read_token().await?;
        let route = self.repo_route(XET_TREE_ROUTE);
        let mut query: Vec<(String, String)> = vec![("prefix".to_owned(), prefix.to_owned())];
        if let Some(limit) = limit {
            query.push(("limit".to_owned(), limit.to_string()));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor".to_owned(), cursor.to_owned()));
        }
        let url = build_url(&self.api_base, &route, &query);
        let body = self
            .send(&retry, token.token, Method::GET, url, None)
            .await?;
        let response: ListResponse =
            serde_json::from_slice(&body).map_err(|error| metadata_parse("list_dir", &error))?;
        Ok(DirListing {
            entries: response
                .entries
                .into_iter()
                .map(|entry| DirEntry {
                    path: entry.path,
                    is_dir: entry.is_dir,
                    file_id: entry.file_id,
                    size: entry.size,
                    updated_at: entry.updated_at,
                })
                .collect(),
            next_cursor: response.next_cursor,
        })
    }

    pub(crate) async fn register_path(
        &self,
        remote: &str,
        file_id: &str,
    ) -> Result<RegisterResult, SdxError> {
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self.repo_route(XET_PATH_ROUTE);
        let url = build_url(&self.api_base, &route, no_query());
        // Substitute the `{*path}` wildcard with the remote path (axum decodes
        // the captured value; encode each segment so special characters survive).
        let url = url.replace("{*path}", &encode_path_segments(remote));
        let body = self
            .send(
                &retry,
                token.token,
                Method::PUT,
                url,
                Some(serde_json::json!({ "fileId": file_id })),
            )
            .await?;
        let response: RegisterResponse = serde_json::from_slice(&body)
            .map_err(|error| metadata_parse("register_path", &error))?;
        Ok(RegisterResult {
            entry: PathEntry {
                path: response.path,
                file_id: response.file_id,
                size: response.size,
                updated_at: response.updated_at,
            },
            created: response.created,
        })
    }

    async fn delete_path(&self, remote: &str, recursive: bool) -> Result<u64, SdxError> {
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self.repo_route(XET_PATH_ROUTE);
        let mut url = build_url(&self.api_base, &route, no_query());
        url = url.replace("{*path}", &encode_path_segments(remote));
        if recursive {
            url.push_str("?recursive=true");
        }
        let body = self
            .send(&retry, token.token, Method::DELETE, url, None)
            .await?;
        let response: DeletePathResponse =
            serde_json::from_slice(&body).map_err(|error| metadata_parse("delete_path", &error))?;
        Ok(response.deleted)
    }
}

impl XetClient {
    /// Resolves `path` to its content `file_id` in the client's revision.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Transfer`] with `NotFound` when the path is not
    /// registered, or another typed error on failure.
    pub async fn resolve_path(&self, path: &str) -> Result<PathEntry, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .resolve_path(path)
            .await
    }

    /// Lists the immediate children of `prefix` (one page, default limit).
    ///
    /// An empty `prefix` lists the repository root.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the request fails or the response is invalid.
    pub async fn list_dir(&self, prefix: &str) -> Result<DirListing, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .list_paged(prefix, None, None)
            .await
    }

    /// Lists one page of `prefix`'s children with keyset pagination.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the request fails or the response is invalid.
    pub async fn list_dir_paged(
        &self,
        prefix: &str,
        cursor: Option<&str>,
    ) -> Result<DirListing, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .list_paged(prefix, None, cursor)
            .await
    }

    /// Lists all children of `prefix` by walking every page, deduplicating by
    /// `entries[].path` (the server may emit a derived directory on two pages).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a page request fails.
    pub async fn list_dir_all(&self, prefix: &str) -> Result<Vec<DirEntry>, SdxError> {
        let client = MetadataClient::from_download(self.download_inner());
        let mut all = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = client.list_paged(prefix, None, cursor.as_deref()).await?;
            for entry in page.entries {
                if seen.insert(entry.path.clone()) {
                    all.push(entry);
                }
            }
            match page.next_cursor {
                Some(next) if next != cursor.as_deref().unwrap_or_default() => cursor = Some(next),
                _ => break,
            }
        }
        Ok(all)
    }

    /// Registers `remote` → `file_id` in the client's revision (auto-creating
    /// the revision), returning the registered entry and whether it was newly
    /// created.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Transfer`] with `BadRequest` when the `file_id` is
    /// not registered in the revision's shards, or another typed error.
    pub async fn register_path(
        &self,
        remote: &str,
        file_id: &str,
    ) -> Result<RegisterResult, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .register_path(remote, file_id)
            .await
    }

    /// Deregisters `remote`, returning the number of paths deleted.
    ///
    /// With `recursive = true` the whole subtree is removed; without it, only
    /// the exact path. Idempotent (a missing path deletes 0).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the request fails.
    pub async fn delete_path(&self, remote: &str, recursive: bool) -> Result<u64, SdxError> {
        MetadataClient::from_download(self.download_inner())
            .delete_path(remote, recursive)
            .await
    }
}

/// Builds a URL from an API base + route template + query pairs, percent-
/// encoding query values (URL-safe, avoids `Url::parse` fallibility).
/// An empty query parameter slice (gives `build_url` a concrete element type).
pub(crate) const fn no_query() -> &'static [(&'static str, &'static str)] {
    &[]
}

pub(crate) fn build_url<K: AsRef<str>, V: AsRef<str>>(
    api_base: &str,
    route: &str,
    query: &[(K, V)],
) -> String {
    let mut url = format!("{}{}", api_base.trim_end_matches('/'), route);
    if !query.is_empty() {
        url.push('?');
        for (index, (key, value)) in query.iter().enumerate() {
            if index > 0 {
                url.push('&');
            }
            url.push_str(key.as_ref());
            url.push('=');
            url.push_str(&encode_query(value.as_ref()));
        }
    }
    url
}

pub(crate) fn encode_query(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Percent-encodes each path segment, preserving `/` separators (so a
/// `{*path}` wildcard captures the raw segments and axum decodes them).
pub(crate) fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(encode_query)
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn metadata_parse(context: &str, error: &serde_json::Error) -> SdxError {
    SdxError::Metadata(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, path_regex, query_param},
    };

    use super::{encode_path_segments, encode_query};
    use crate::{Auth, RepositoryId, XetClientBuilder};

    const READ_TOKEN: &str = "read-token";
    const WRITE_TOKEN: &str = "write-token";
    const BOOTSTRAP_KEY: &str = "bootstrap";

    async fn build_client(server: &MockServer) -> crate::XetClient {
        let auth = Auth::new(
            &server.uri(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_api_key(BOOTSTRAP_KEY.to_owned())
        .with_subject("user".to_owned());
        let port = server.uri().split(':').next_back().unwrap().to_owned();
        XetClientBuilder::new()
            .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
            .auth(auth)
            .build()
            .unwrap()
    }

    async fn mock_read_token(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-read-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": READ_TOKEN,
            })))
            .mount(server)
            .await;
    }

    async fn mock_write_token(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-write-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": WRITE_TOKEN,
            })))
            .mount(server)
            .await;
    }

    #[test]
    fn encode_query_escapes_reserved_characters() {
        assert_eq!(encode_query("data/model.pt"), "data%2Fmodel.pt");
        assert_eq!(encode_query("a b.txt"), "a+b.txt");
        assert_eq!(encode_query("plain"), "plain");
    }

    #[test]
    fn encode_path_segments_preserves_separators() {
        assert_eq!(encode_path_segments("data/model.pt"), "data/model.pt");
        assert_eq!(encode_path_segments("a b/c.txt"), "a+b/c.txt");
    }

    /// Verifies keyset pagination walking: `list_dir_all` follows the cursor
    /// across pages and deduplicates by path (a derived dir may appear on two
    /// pages).
    #[tokio::test]
    async fn list_dir_all_walks_pages_and_dedups() {
        let server = MockServer::start().await;
        mock_read_token(&server).await;
        // Page 2 (cursor=z) mounted first so first-match-wins handles it.
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/tree/main"))
            .and(query_param("cursor", "z"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "entries": [
                    {"path": "b.txt", "isDir": false, "fileId": "b", "size": 1, "updatedAt": 1},
                    {"path": "dir/", "isDir": true, "fileId": null, "size": null, "updatedAt": null}
                ],
                "nextCursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/tree/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "entries": [
                    {"path": "a.txt", "isDir": false, "fileId": "a", "size": 1, "updatedAt": 1},
                    {"path": "dir/", "isDir": true, "fileId": null, "size": null, "updatedAt": null}
                ],
                "nextCursor": "z"
            })))
            .mount(&server)
            .await;

        let client = build_client(&server).await;
        let all = client.list_dir_all("").await.unwrap();
        // `dir/` appears on both pages but is deduplicated.
        let paths: Vec<String> = all.iter().map(|entry| entry.path.clone()).collect();
        let unique: HashSet<String> = paths.iter().cloned().collect();
        assert_eq!(
            paths.len(),
            unique.len(),
            "list_dir_all must deduplicate: {paths:?}"
        );
        assert!(all.iter().any(|entry| entry.path == "a.txt"));
        assert!(all.iter().any(|entry| entry.path == "b.txt"));
        assert!(all.iter().any(|entry| entry.path == "dir/" && entry.is_dir));
        // 3 unique entries (a.txt, b.txt, dir/).
        assert_eq!(all.len(), 3);
    }

    /// Verifies a 403 scope cross-check surfaces as a typed Forbidden error
    /// (M4 refresh-once then surface).
    #[tokio::test]
    async fn register_path_403_surfaces_forbidden() {
        let server = MockServer::start().await;
        mock_write_token(&server).await;
        Mock::given(method("PUT"))
            .and(path_regex(r"/api/github/team/assets/path/main/.*"))
            .respond_with(
                ResponseTemplate::new(403).set_body_json(json!({"error": "insufficient scope"})),
            )
            .mount(&server)
            .await;

        let client = build_client(&server).await;
        let err = client
            .register_path("a/b.txt", &"0".repeat(64))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdxError::Transfer(crate::error::TransferError::Forbidden(_))
        ));
    }

    /// Verifies a 404 resolve surfaces as NotFound (not an error type).
    #[tokio::test]
    async fn resolve_path_404_surfaces_not_found() {
        let server = MockServer::start().await;
        mock_read_token(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/tree/main"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "not found"})))
            .mount(&server)
            .await;

        let client = build_client(&server).await;
        let err = client.resolve_path("missing.txt").await.unwrap_err();
        assert!(matches!(
            err,
            crate::error::SdxError::Transfer(crate::error::TransferError::NotFound(_))
        ));
    }
}
