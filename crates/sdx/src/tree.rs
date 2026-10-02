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

// Server metadata_routes accepts <=4096-byte paths without controls or
// backslashes. Quotes need at most two JSON bytes; derived directory markers
// get one additional byte. Fixed fields include a 64-hex ID and two u64s.
const PATH_RESPONSE_LIMIT: usize = 8_450;

fn tree_page_response_limit(limit: Option<usize>) -> usize {
    // The server defaults to 1000 and rejects page sizes above 10,000.
    let entries = limit.unwrap_or(1000).min(10_000);
    entries
        .saturating_mul(PATH_RESPONSE_LIMIT)
        .saturating_add(8_450)
}

// ── wire response shapes (camelCase) ────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
struct ResolveResponse {
    path: String,
    file_id: String,
    size: u64,
    updated_at: u64,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
struct ListEntry {
    path: String,
    is_dir: bool,
    file_id: Option<String>,
    size: Option<u64>,
    updated_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    entries: Vec<ListEntry>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
struct RegisterResponse {
    path: String,
    file_id: String,
    size: u64,
    updated_at: u64,
    created: bool,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
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
            .replace(
                "{provider}",
                &encode_path_segment(&self.repository.provider),
            )
            .replace("{owner}", &encode_path_segment(&self.repository.owner))
            .replace("{repo}", &encode_path_segment(&self.repository.repo))
            .replace("{rev}", &encode_path_segment(&self.repository.revision))
    }

    /// Substitutes only the repo-scope placeholders (`{provider}/{owner}/{repo}`),
    /// leaving `{rev}` for a per-call revision argument (used by the revision
    /// create/delete routes, whose revision is not the client's default).
    pub(crate) fn repo_route_scope(&self, template: &str) -> String {
        template
            .replace(
                "{provider}",
                &encode_path_segment(&self.repository.provider),
            )
            .replace("{owner}", &encode_path_segment(&self.repository.owner))
            .replace("{repo}", &encode_path_segment(&self.repository.repo))
    }

    /// Retries HTTP failures, then returns an admitted/decoded metadata DTO.
    pub(crate) async fn send_json<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        retry: &RetryContext,
        token: String,
        method: Method,
        url: String,
        body: Option<serde_json::Value>,
        response: (&'static str, usize),
    ) -> Result<T, SdxError> {
        let transfer = self.transfer.clone();
        let decoded = retry
            .run(token, move |tok| {
                let transfer = transfer.clone();
                let url = url.clone();
                let method = method.clone();
                let body = body.clone();
                async move {
                    transfer
                        .request_json::<T>(&method, &url, &tok, body.as_ref(), response.1)
                        .await
                }
            })
            .await?;
        decoded.map_err(|error| metadata_parse(response.0, &error))
    }

    async fn resolve_path(&self, path: &str) -> Result<PathEntry, SdxError> {
        let retry = self.read_retry();
        let token = self.tokens.read_token().await?;
        let route = self.repo_route(XET_TREE_ROUTE);
        let url = build_url(&self.api_base, &route, &[("path", path)]);
        let response: ResolveResponse = self
            .send_json(
                &retry,
                token.token,
                Method::GET,
                url,
                None,
                ("resolve_path", PATH_RESPONSE_LIMIT),
            )
            .await?;

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
        let response: ListResponse = self
            .send_json(
                &retry,
                token.token,
                Method::GET,
                url,
                None,
                ("list_dir", tree_page_response_limit(limit)),
            )
            .await?;

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
        validate_mutation_path(remote)?;
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self.repo_route(XET_PATH_ROUTE);
        let url = build_url(&self.api_base, &route, no_query());
        // Substitute the `{*path}` wildcard with the remote path (axum decodes
        // the captured value; encode each segment so special characters survive).
        let url = url.replace("{*path}", &encode_path_segments(remote));
        let response: RegisterResponse = self
            .send_json(
                &retry,
                token.token,
                Method::PUT,
                url,
                Some(serde_json::json!({ "fileId": file_id })),
                ("register_path", PATH_RESPONSE_LIMIT),
            )
            .await?;

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
        validate_mutation_path(remote)?;
        let retry = self.write_retry();
        let token = self.tokens.write_token().await?;
        let route = self.repo_route(XET_PATH_ROUTE);
        let mut url = build_url(&self.api_base, &route, no_query());
        url = url.replace("{*path}", &encode_path_segments(remote));
        if recursive {
            url.push_str("?recursive=true");
        }
        let response: DeletePathResponse = self
            .send_json(
                &retry,
                token.token,
                Method::DELETE,
                url,
                None,
                ("delete_path", PATH_RESPONSE_LIMIT),
            )
            .await?;

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
    /// Returns [`SdxError`] when a page request fails or a cursor is empty or repeats.
    pub async fn list_dir_all(&self, prefix: &str) -> Result<Vec<DirEntry>, SdxError> {
        let client = MetadataClient::from_download(self.download_inner());
        let mut all = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        loop {
            let page = client.list_paged(prefix, None, cursor.as_deref()).await?;
            for entry in page.entries {
                if seen.insert(entry.path.clone()) {
                    all.push(entry);
                }
            }
            cursor = checked_next_cursor("list_dir_all", page.next_cursor, &mut cursors)?;
            if cursor.is_none() {
                break;
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
///
/// Unlike [`encode_query`], this uses RFC-3986 path-segment encoding rather than
/// `form_urlencoded::byte_serialize`. The query encoder treats a space as `+`
/// (valid only in query strings), but path segments are NOT query strings: axum's
/// `Path` percent-decodes segments and treats `+` literally, so using the query
/// encoder would register `a b.txt` as `a+b.txt` and the server would then
/// register/delete the mangled path. Path-segment encoding keeps `+` literal and
/// encodes a space as `%20`, so `register_path`/`delete_path`/`list_dir`/
/// `resolve_path` all round-trip the same path.
/// RFC 3986 path-segment character set (`pchar`): unreserved chars plus
/// sub-delims plus `:` and `@`. Everything else (space, `%`, non-ASCII, ...)
/// is percent-encoded. In particular `+` (a sub-delim) stays literal, which is
/// exactly the behavior `encode_path_segments` needs to avoid mangling spaces
/// into `+`. percent-encoding 2.x does not ship this set, so it is built from
/// `NON_ALPHANUMERIC` by un-encoding the permitted characters.
const PATH_SEGMENT_PCHAR: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'!')
    .remove(b'$')
    .remove(b'&')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')')
    .remove(b'*')
    .remove(b'+')
    .remove(b',')
    .remove(b';')
    .remove(b'=')
    .remove(b':')
    .remove(b'@');

pub(crate) fn encode_path_segment(segment: &str) -> String {
    percent_encoding::utf8_percent_encode(segment, PATH_SEGMENT_PCHAR).to_string()
}

pub(crate) fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// HTTP URL parsers remove literal dot segments before the server can reject
/// them. Never let a malformed mutation path target a different registered key.
fn validate_mutation_path(remote: &str) -> Result<(), SdxError> {
    if remote
        .split('/')
        .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(SdxError::Metadata(
            "path mutation: dot segments are not allowed".to_owned(),
        ));
    }
    Ok(())
}

/// Only an absent cursor denotes exhaustion. Reject malformed/repeated cursors
/// rather than returning a partial listing or looping forever on a cycle.
pub(crate) fn checked_next_cursor(
    context: &str,
    next: Option<String>,
    seen: &mut HashSet<String>,
) -> Result<Option<String>, SdxError> {
    if let Some(cursor) = &next
        && (cursor.is_empty() || !seen.insert(cursor.clone()))
    {
        return Err(SdxError::Metadata(format!(
            "{context}: empty or repeated pagination cursor"
        )));
    }
    Ok(next)
}

pub(crate) fn metadata_parse(context: &str, error: &impl std::fmt::Display) -> SdxError {
    SdxError::Metadata(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn metadata_response_envelopes_cover_maximum_legal_paths_and_pages() {
        let path = "\"".repeat(4096);
        let entry = super::ListEntry {
            path: path.clone(),
            is_dir: false,
            file_id: Some("a".repeat(64)),
            size: Some(u64::MAX),
            updated_at: Some(u64::MAX),
        };
        let resolve = super::ResolveResponse {
            path: path.clone(),
            file_id: "a".repeat(64),
            size: u64::MAX,
            updated_at: u64::MAX,
        };
        let register = super::RegisterResponse {
            path: path.clone(),
            file_id: "a".repeat(64),
            size: u64::MAX,
            updated_at: u64::MAX,
            created: false,
        };
        assert!(serde_json::to_vec(&resolve).unwrap().len() <= super::PATH_RESPONSE_LIMIT);
        assert!(serde_json::to_vec(&register).unwrap().len() <= super::PATH_RESPONSE_LIMIT);
        let delete = serde_json::json!({ "path": &path, "deleted": u64::MAX, "recursive": false });
        assert!(serde_json::to_vec(&delete).unwrap().len() <= super::PATH_RESPONSE_LIMIT);
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Page<'wire> {
            entries: Vec<&'wire super::ListEntry>,
            next_cursor: Option<&'wire str>,
        }
        struct Count(usize);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("byte counter overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for limit in [None, Some(1), Some(10_000)] {
            let page = Page {
                entries: vec![&entry; limit.unwrap_or(1000)],
                next_cursor: Some(&path),
            };
            let mut count = Count(0);
            serde_json::to_writer(&mut count, &page).unwrap();
            assert!(count.0 <= super::tree_page_response_limit(limit));
        }
    }

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

    #[tokio::test]
    async fn resolve_json_decode_preserves_context_without_retrying_parse_errors() {
        let server = MockServer::start().await;
        mock_read_token(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/tree/main"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{invalid"))
            .expect(1)
            .mount(&server)
            .await;
        let error = build_client(&server)
            .await
            .resolve_path("file.txt")
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::SdxError::Metadata(message) if message.starts_with("resolve_path: "))
        );
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
        assert_eq!(encode_path_segments("a b/c.txt"), "a%20b/c.txt");
    }

    #[tokio::test]
    async fn mutation_dot_segments_never_reach_another_remote_path() {
        let server = MockServer::start().await;
        mock_write_token(&server).await;
        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/github/team/assets/path/main/.*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"deleted": 1})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path_regex(r"/api/github/team/assets/path/main/.*"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "path": "victim", "fileId": "0".repeat(64), "size": 0,
                "updatedAt": 1, "created": false,
            })))
            .mount(&server)
            .await;
        let client = build_client(&server).await;
        for remote in ["directory/../victim", "directory/./victim", ".", ".."] {
            assert!(matches!(
                client.delete_path(remote, false).await,
                Err(crate::SdxError::Metadata(_))
            ));
            assert!(matches!(
                client.register_path(remote, &"0".repeat(64)).await,
                Err(crate::SdxError::Metadata(_))
            ));
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn encoded_endpoint_identity_survives_builder_auth_and_metadata_routes() {
        let server = MockServer::start().await;
        let revision = "release candidate+/%#?版本";
        let repository = RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: revision.to_owned(),
        };
        for endpoint in ["xet-read-token", "tree"] {
            let expected_revision = revision.to_owned();
            let response = if endpoint == "tree" {
                json!({"entries": [], "nextCursor": null})
            } else {
                json!({"casUrl": server.uri(), "exp": 4_000_000_000u64, "accessToken": READ_TOKEN})
            };
            Mock::given(method("GET"))
                .and(move |request: &wiremock::Request| {
                    request
                        .url
                        .path()
                        .strip_prefix(&format!("/api/github/team/assets/{endpoint}/"))
                        .is_some_and(|segment| {
                            !segment.contains('/')
                                && percent_encoding::percent_decode_str(segment)
                                    .decode_utf8()
                                    .is_ok_and(|decoded| decoded == expected_revision)
                        })
                })
                .respond_with(ResponseTemplate::new(200).set_body_json(response))
                .expect(1)
                .mount(&server)
                .await;
        }
        let auth = Auth::new(&server.uri(), repository)
            .unwrap()
            .with_api_key(BOOTSTRAP_KEY.to_owned());
        let endpoint = crate::XetUrl::parse(&format!(
            "xet://127.0.0.1:{}/github/team/assets/release%20candidate+%2F%25%23%3F%E7%89%88%E6%9C%AC",
            server.address().port()
        )).unwrap();
        let client = XetClientBuilder::new()
            .endpoint(endpoint.endpoint_url())
            .auth(auth)
            .build()
            .unwrap();
        assert!(client.list_dir_all("").await.unwrap().is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn pagination_rejects_empty_repeated_and_cyclic_cursors() {
        for returned_cursors in [vec![""], vec!["a", "a"], vec!["a", "b", "a"]] {
            let server = MockServer::start().await;
            mock_read_token(&server).await;
            for (index, next) in returned_cursors.iter().enumerate() {
                let expected = index.checked_sub(1).map(|i| returned_cursors[i].to_owned());
                Mock::given(method("GET"))
                    .and(path("/api/github/team/assets/tree/main"))
                    .and(move |request: &wiremock::Request| {
                        request
                            .url
                            .query_pairs()
                            .find(|(key, _)| key == "cursor")
                            .map(|(_, value)| value.into_owned())
                            == expected
                    })
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "entries": [], "nextCursor": next
                    })))
                    .expect(1)
                    .mount(&server)
                    .await;
            }
            let client = build_client(&server).await;
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(2), client.list_dir_all(""))
                    .await
                    .expect("malformed pagination must terminate");
            assert!(
                matches!(result, Err(crate::SdxError::Metadata(_))),
                "{result:?}"
            );
            server.verify().await;
        }
    }

    /// A path segment is NOT a query string: axum's `Path` percent-decodes and
    /// treats `+` literally, so it must stay literal here and spaces must become
    /// `%20`. This guarantees `register_path`/`delete_path`/`list_dir`/`resolve_path`
    /// all encode the same way and round-trip back to the original path.
    #[test]
    fn encode_path_segments_round_trips_spaces_plus_percent_unicode() {
        // Note: `a b.txt` (with a literal space) must NOT become `a+b.txt`.
        let original = "a b.txt";
        let encoded = encode_path_segments(original);
        assert_eq!(encoded, "a%20b.txt");
        assert_ne!(
            encoded, "a+b.txt",
            "space must not be encoded as '+' in a path segment"
        );

        // `+` in a path is literal data, not a space.
        assert_eq!(encode_path_segments("x+y.txt"), "x+y.txt");

        // `%` must be percent-encoded so the server-side decode round-trips
        // instead of treating the literal `%` as an escape prefix.
        assert_eq!(encode_path_segments("100%done.txt"), "100%25done.txt");

        // Non-ASCII round-trips through UTF-8 percent-encoding.
        assert_eq!(encode_path_segments("café.txt"), "caf%C3%A9.txt");

        // Multi-segment round-trip: encode each segment, then percent-decode
        // each decoded segment to reconstruct the original path.
        let path = "dir with space/sub dir/100% caf\u{e9}+x.txt";
        let encoded = encode_path_segments(path);
        let round_tripped = encoded
            .split('/')
            .map(|segment| {
                percent_encoding::percent_decode_str(segment)
                    .decode_utf8()
                    .expect("encoded segments must be valid UTF-8")
                    .into_owned()
            })
            .collect::<Vec<_>>()
            .join("/");
        assert_eq!(round_tripped, path);
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
