use axum::extract::{FromRequestParts, RawPathParams};
use axum::http::HeaderMap;
use axum::http::request::Parts;

use crate::error::HubApiError;
use crate::models::RepoType;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;
use shardline_server_core::{AuthorizedRepository, VerifiedAuthContext};

use super::HubState;

/// Authorize the request if auth is configured. Returns `Ok(())` when no auth
/// is set (permissive) or when the token satisfies the required scope.
pub(crate) fn authorize(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<(), HubApiError> {
    if let Some(auth) = &state.auth {
        auth.authorize(headers, required_scope)?;
    }
    Ok(())
}

/// Authorize the request and return the verified auth context, when auth is
/// configured.
///
/// Returns `Ok(None)` in permissive mode (no auth configured) and
/// `Ok(Some(VerifiedAuthContext))` when a valid token was presented. This lets
/// repo-scoped handlers enforce a token→repository binding in addition to the
/// scope check performed by [`authorize`].
///
/// # Errors
///
/// Returns [`HubApiError::Unauthorized`] or [`HubApiError::InvalidToken`] on a
/// missing/invalid token, or [`HubApiError::Forbidden`] on insufficient scope.
pub(crate) fn authorize_with_context(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<VerifiedAuthContext>, HubApiError> {
    if let Some(auth) = &state.auth {
        Ok(Some(auth.authorize(headers, required_scope)?))
    } else {
        Ok(None)
    }
}

/// Requires that the authenticated token's repository scope matches the
/// request's `(ns, repo)` URL-path segment.
///
/// This is the hub-api counterpart to the core layer's
/// `validate_oci_repository_scope` / `scope_namespace` binding: a token issued
/// for repo `owner/name` must only ever act on that same repo. Without this
/// binding, any authenticated user could read/write/delete/git-push another
/// tenant's repository using a token scoped only by `scope` (Read/Write).
///
/// In permissive mode (`auth_ctx` is `None`, i.e. no auth is configured) the
/// check is a no-op so development deployments without auth keep working.
///
/// # Errors
///
/// Returns [`HubApiError::Forbidden`] (HTTP 403) when the token's repository
/// does not match the requested `(ns, repo)`.
pub(crate) fn require_repository_binding(
    auth_ctx: Option<&VerifiedAuthContext>,
    ns: &str,
    repo: &str,
) -> Result<(), HubApiError> {
    let Some(ctx) = auth_ctx else {
        return Ok(());
    };
    let claims_repo = ctx.claims().repository();
    if claims_repo.owner() == ns && claims_repo.name() == repo {
        Ok(())
    } else {
        Err(HubApiError::Forbidden)
    }
}

/// Converts a `HubRepoType` to the API path string.
///
/// Delegates to [`RepoType::as_path_str`] so the plural path segment mapping
/// lives in a single place.
pub(crate) fn repo_type_path(rt: HubRepoType) -> &'static str {
    RepoType::from(rt).as_path_str()
}

/// Typed, repo-scoped authorization capability extractor.
///
/// This is the **only** way a repo-scoped Hub route handler can obtain an
/// [`AuthorizedRepository`]: it reproduces the exact auth chain the handlers
/// used to perform inline, in the same order:
///
/// 1. parse `(ns, repo)` from the request path segments;
/// 2. [`authorize_with_context`] — 401 on missing/invalid credentials, 403 on
///    insufficient scope (permissive `Ok(None)` when no auth is configured);
/// 3. [`require_repository_binding`] — 403 when the token's repository scope
///    does not match the URL's `(ns, repo)`; no-op when auth is `None`;
/// 4. mint the capability: `Some(ctx)` → [`AuthorizedRepository::from_verified_context`],
///    `None` → [`AuthorizedRepository::anonymous_full_access`] (binding skipped,
///    global namespace — reproducing today's permissive no-op exactly).
///
/// The const generic parameter `WRITE` selects the required scope:
/// `HubRepository` (default, read-scoped) vs `HubRepository<true>` (write-scoped).
/// `BIND` controls whether the token→repository binding is enforced; it is
/// disabled only for the deliberately global repo-create routes.
///
/// Pathless routes (LFS, repo list/search, repo create) carry no `(ns, repo)`
/// in the URL; their isolation comes from the token claims carried by the
/// capability, so the binding is skipped for them.
#[derive(Debug, Clone)]
pub struct HubRepository<const WRITE: bool = false, const BIND: bool = true> {
    /// The verified capability. Handlers must pass this (or its
    /// `repository()`/`claims()`) wherever repo-scoped state is touched.
    pub(crate) capability: AuthorizedRepository,
}

impl<const WRITE: bool, const BIND: bool> HubRepository<WRITE, BIND> {
    const fn required_scope() -> TokenScope {
        if WRITE {
            TokenScope::Write
        } else {
            TokenScope::Read
        }
    }

    /// Runs the extractor chain against already-parsed request parts.
    async fn from_request(parts: &mut Parts, state: &HubState) -> Result<Self, HubApiError> {
        let required_scope = Self::required_scope();
        // 1. (ns, repo) from the router's matched path params — the single
        //    source of truth for the repository the handler will read from.
        let ns_repo = extract_repo_path_params(parts, state).await;
        // 2. Authorize (permissive `Ok(None)` when no auth is configured).
        let auth_ctx = authorize_with_context(state, &parts.headers, required_scope)?;
        // 3. Enforce the token→repository binding (no-op when auth is None,
        //    when the route is pathless, or when BIND is disabled).
        if BIND && let Some((ns, repo)) = &ns_repo {
            require_repository_binding(auth_ctx.as_ref(), ns, repo)?;
        }
        // 4. Mint the capability.
        let capability = match auth_ctx {
            Some(ctx) => AuthorizedRepository::from_verified_context(ctx, required_scope)?,
            None => AuthorizedRepository::anonymous_full_access(),
        };
        Ok(Self { capability })
    }

    /// Returns the authorized repository capability.
    #[must_use]
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.capability
    }

    /// Requires that this capability's repository scope matches `ns/repo`.
    ///
    /// This is the capability-based counterpart of [`require_repository_binding`]
    /// for handlers whose repository identity comes from the request body
    /// (e.g. the compat repo-delete endpoint) rather than the URL path. It is a
    /// no-op for anonymous (permissive-mode) capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::Forbidden`] when the capability's repository does
    /// not match `ns/repo`.
    pub(crate) fn require_binding(&self, ns: &str, repo: &str) -> Result<(), HubApiError> {
        let Some(claims) = self.capability.claims() else {
            return Ok(());
        };
        let claims_repo = claims.repository();
        if claims_repo.owner() == ns && claims_repo.name() == repo {
            Ok(())
        } else {
            Err(HubApiError::Forbidden)
        }
    }
}

impl<const WRITE: bool, const BIND: bool> FromRequestParts<HubState>
    for HubRepository<WRITE, BIND>
{
    type Rejection = HubApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HubState,
    ) -> Result<Self, Self::Rejection> {
        Self::from_request(parts, state).await
    }
}

/// Known repo-type path segments (singular and plural API forms).
const REPO_TYPE_SEGMENTS: &[&str] = &["model", "models", "dataset", "datasets", "space", "spaces"];

/// Test helper: runs the real authorize path against `state` and mints a
/// capability, mirroring what the [`HubRepository`] extractor does. Permissive
/// states produce an anonymous full-access capability; configured auth
/// providers are exercised through the real `authorize_with_context` chain.
#[cfg(test)]
pub(crate) fn test_repo<const WRITE: bool, const BIND: bool>(
    state: &HubState,
    headers: &HeaderMap,
) -> HubRepository<WRITE, BIND> {
    let required_scope = if WRITE {
        TokenScope::Write
    } else {
        TokenScope::Read
    };
    let ctx = authorize_with_context(state, headers, required_scope).expect("test auth");
    let capability = ctx.map_or_else(AuthorizedRepository::anonymous_full_access, |ctx| {
        AuthorizedRepository::from_verified_context(ctx, required_scope).expect("test scope")
    });
    HubRepository { capability }
}

/// Known "verb" segments that immediately follow a repo-scoped `(ns, repo)`
/// pair in the Hub router's path space.
const REPO_VERB_SEGMENTS: &[&str] = &[
    "revision",
    "modelcard",
    "revisions",
    "preupload",
    "commit",
    "tree",
    "webhooks",
    "resolve",
    "info",
    "HEAD",
    "git-upload-pack",
    "git-receive-pack",
    "parquet",
    "first-rows",
    "viewer",
];

/// Extracts the `(ns, repo)` pair from a repo-scoped request path.
///
/// Recognizes every repo-scoped route family served by the Hub router:
///
/// - `{type}/{ns}/{repo}/<verb>[/...]` and `/api/{type}/{ns}/{repo}/<verb>[/...]`
/// - `/api/datasets/{ns}/{repo}/<verb>[/...]` (no repo-type segment)
/// - `/{ns}/{repo}/resolve/...` (root-level model resolve)
/// - `/api/{type}/{ns}/{repo}` (the path ends at the repository itself)
///
/// Returns `None` for pathless routes (`/objects/batch`, `/lfs/objects/{oid}`,
/// `/api/repos`, `/api/{type}/search`, `/api/repos/create`, `/api/repos/delete`)
/// whose isolation comes from the token claims rather than a URL binding.
fn extract_repo_path(path: &str) -> Option<(String, String)> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // `/api/{type}/{ns}/{repo}` — the path ends at the repository. Prefer this
    // shape FIRST: it is the single source of truth for a repository that ends
    // the path, and it must win even when the repo's name collides with a route
    // verb (e.g. `/api/models/ns/commit` is the repository `ns/commit`, NOT a
    // commit of `models/ns`). The segment before the pair must be a repo type
    // to distinguish this from pathless route families (`/lfs/objects/{oid}`,
    // `/api/repos`, ...).
    //
    // The rule only applies when the path ENDS at the repository — exactly
    // `/{type}/{ns}/{repo}` (3 segments) or `/api/{type}/{ns}/{repo}` (4
    // segments). A deep `{*path}` tail (tree/resolve) must never shift which
    // three segments are interpreted as `[type, ns, repo]`: e.g.
    // `/api/models/bob/own/tree/main/datasets/alice/own` must bind to
    // `bob/own`, not to the trailing `[datasets, alice, own]` (F-104).
    let ends_at_repo =
        segments.len() == 3 || (segments.len() == 4 && segments.first() == Some(&"api"));
    if ends_at_repo
        && let [repo_type, ns, repo] = segments.get(segments.len().checked_sub(3)?..)?
        && REPO_TYPE_SEGMENTS.contains(repo_type)
    {
        return Some((ns.to_string(), repo.to_string()));
    }
    // `(ns, repo)` is the pair immediately preceding a known verb segment. This
    // is only reached when the path does NOT end at the repository. Scan
    // BACKWARD and take the LAST verb-followed window: the router's verbs
    // always directly follow the `(ns, repo)` pair, so the last window wins
    // even when an earlier window would also end in a verb — e.g. a repo named
    // `commit` at `/api/models/ns/commit/commit/{rev}` must bind to
    // `ns/commit`, not to `models/ns` from the `[models, ns, commit]` window.
    for window in segments.windows(3).rev() {
        let [ns, repo, verb] = window else { continue };
        if REPO_VERB_SEGMENTS.contains(verb) {
            return Some((ns.to_string(), repo.to_string()));
        }
    }
    None
}

/// Extracts the `(ns, repo)` pair from the router's matched path params.
///
/// The router's `Path` params are the single source of truth for the
/// repository a handler will read from: they are derived from the route
/// pattern (`{ns}/{repo}`), so a deep `{*path}` tail (tree/resolve) can
/// never shift which segments are interpreted as the repository pair. This
/// replaces the URI re-parse in [`extract_repo_path`], which misparsed deep
/// tails whose last three segments happened to look like `[type, ns, repo]`
/// (F-104).
///
/// Returns `None` for pathless routes (no `ns`/`repo` params), whose
/// isolation comes from the token claims rather than a URL binding.
async fn extract_repo_path_params(parts: &mut Parts, state: &HubState) -> Option<(String, String)> {
    let params = match RawPathParams::from_request_parts(parts, state).await {
        Ok(params) => params,
        // No matched params (pathless route) or a param with invalid UTF-8:
        // fall back to the URI re-parse, which returns `None` for pathless
        // routes and is only reached when the router matched no `ns`/`repo`.
        Err(_) => return extract_repo_path(parts.uri.path()),
    };
    let mut ns = None;
    let mut repo = None;
    for (key, value) in &params {
        match key {
            "ns" => ns = Some(value.to_owned()),
            "repo" => repo = Some(value.to_owned()),
            _ => {}
        }
    }
    match (ns, repo) {
        (Some(ns), Some(repo)) => Some((ns, repo)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::extract_repo_path;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header::AUTHORIZATION};
    use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthError, AuthProvider};
    use tower::ServiceExt;

    use crate::auth::HubAuth;
    use crate::routes::HubState;

    #[test]
    fn extract_repo_path_repo_named_like_verb_is_parsed_as_repository() {
        // A repository whose name collides with a route verb must parse as the
        // last-3 [type, ns, repo] shape, not as a verb-window over the type.
        for (path, expected) in [
            ("/api/models/ns/commit", ("ns", "commit")),
            ("/api/models/ns/tree", ("ns", "tree")),
            ("/api/models/ns/webhooks", ("ns", "webhooks")),
            ("/api/models/ns/resolve", ("ns", "resolve")),
            ("/api/models/ns/HEAD", ("ns", "HEAD")),
            ("/api/models/ns/info", ("ns", "info")),
            ("/api/models/ns/revisions", ("ns", "revisions")),
            (
                "/api/models/ns/git-receive-pack",
                ("ns", "git-receive-pack"),
            ),
            ("/api/datasets/ns/parquet", ("ns", "parquet")),
        ] {
            let parsed = extract_repo_path(path);
            assert_eq!(
                parsed,
                Some((expected.0.to_owned(), expected.1.to_owned())),
                "path {path} must bind to {expected:?}"
            );
        }
    }

    #[test]
    fn extract_repo_path_verb_after_non_repo_type_segment() {
        // A path that ends at the repository (no trailing verb) still binds to
        // the last-3 shape, even when the repo-type segment is the pair head.
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/datasets/ns/my-repo"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_verb_window_still_works_for_trailing_verbs() {
        // When the path continues past the repository with a real verb, the
        // verb-window rule still resolves the (ns, repo) pair.
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/commit/abc123"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/tree/main/path/file.txt"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/datasets/ns/my-repo/parquet"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/my-repo/webhooks"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
        // Root-level model resolve.
        assert_eq!(
            extract_repo_path("/ns/my-repo/resolve/some/name"),
            Some(("ns".to_owned(), "my-repo".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_verb_collision_nested_route_binds_to_repository() {
        // A repo named `commit` used through a route whose verb is also
        // `commit` (`/api/models/{ns}/{repo}/commit/{rev}`): the LAST
        // verb-followed window is `[ns, commit, commit]`, not the type-prefixed
        // `[models, ns, commit]` window.
        assert_eq!(
            extract_repo_path("/api/models/ns/commit/commit/main"),
            Some(("ns".to_owned(), "commit".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/tree/tree/main/a/b"),
            Some(("ns".to_owned(), "tree".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/webhooks/webhooks/abc"),
            Some(("ns".to_owned(), "webhooks".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/revisions/revisions"),
            Some(("ns".to_owned(), "revisions".to_owned()))
        );
        // Root-level git smart-HTTP for a repo named `commit`.
        assert_eq!(
            extract_repo_path("/models/ns/commit/git-receive-pack"),
            Some(("ns".to_owned(), "commit".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_repo_named_like_repo_type_segment() {
        // A repo literally named "models" must still bind as the last pair.
        assert_eq!(
            extract_repo_path("/api/models/ns/models"),
            Some(("ns".to_owned(), "models".to_owned()))
        );
        assert_eq!(
            extract_repo_path("/api/models/ns/models/revisions"),
            Some(("ns".to_owned(), "models".to_owned()))
        );
    }

    #[test]
    fn extract_repo_path_pathless_routes_return_none() {
        for path in [
            "/objects/batch",
            "/lfs/objects/0123456789abcdef0123456789abcdef",
            "/api/repos",
            "/api/models/search",
            "/api/repos/create",
            "/api/repos/delete",
        ] {
            assert_eq!(extract_repo_path(path), None, "path {path} is pathless");
        }
    }

    // -----------------------------------------------------------------------
    // F-104 regression: a deep `{*path}` tail must never shift which three
    // segments are interpreted as `[type, ns, repo]`.
    // -----------------------------------------------------------------------

    #[test]
    fn extract_repo_path_deep_tail_does_not_shift_repo_pair() {
        // The tree route's tail `datasets/alice/own` must not rebind the
        // request to `alice/own`: the repository is `bob/own`.
        assert_eq!(
            extract_repo_path("/api/models/bob/own/tree/main/datasets/alice/own"),
            Some(("bob".to_owned(), "own".to_owned()))
        );
        // Same for the root-level resolve route.
        assert_eq!(
            extract_repo_path("/models/bob/own/resolve/main/datasets/foo/bar"),
            Some(("bob".to_owned(), "own".to_owned()))
        );
        // A repo literally named `models` still binds as the last pair when the
        // path ends at the repository (F-37 behavior preserved).
        assert_eq!(
            extract_repo_path("/api/models/alice/models"),
            Some(("alice".to_owned(), "models".to_owned()))
        );
    }

    /// Auth provider that verifies any token as scoped to `ns/repo` with Read
    /// scope — the minimal surface the repo-binding regression tests need.
    struct ScopedProvider {
        ns: &'static str,
        repo: &'static str,
    }

    impl AuthProvider for ScopedProvider {
        fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
            let repo = RepositoryScope::new(RepositoryProvider::Generic, self.ns, self.repo, None)
                .map_err(|_err| AuthError::InvalidToken)?;
            TokenClaims::new("issuer", self.ns, TokenScope::Read, repo, u64::MAX)
                .map_err(|_err| AuthError::InvalidToken)
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            Ok("test-token".into())
        }
    }

    /// Builds an auth-configured `HubState` whose provider scopes every token
    /// to `ns/repo`, with that repository created and holding one revision.
    fn scoped_state(ns: &'static str, repo: &'static str) -> (tempfile::TempDir, HubState) {
        let ts = tempfile::tempdir().expect("tempdir");
        let root = ts.path();
        shardline_index::hub::ensure_hub_tables(root).expect("ensure hub tables");
        let store = shardline_index::LocalIndexStore::open(root.to_path_buf());
        let boxed = BoxedHubStore::from_store(store);
        let repo_id = format!("{ns}/{repo}");
        boxed
            .create_repo(HubRepoType::Model, &repo_id, false)
            .expect("create repo");
        let parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        boxed
            .create_revision(&repo_id, Some(parent), "sha1", "main", "first")
            .expect("create revision");
        boxed
            .store_files(
                "sha1",
                &[HubFileEntry {
                    path: "README.md".into(),
                    size: 100,
                    sha: "sha_readme".into(),
                    is_lfs: false,
                }],
            )
            .expect("store files");
        let object_store = shardline_server_core::ServerObjectStore::local(root.join("lfs"))
            .expect("local object store");
        let state = HubState {
            store: boxed,
            object_store,
            auth: Some(HubAuth::new(Box::new(ScopedProvider { ns, repo }))),
            http_client: None,
            webhook_secret_cipher: None,
        };
        (ts, state)
    }

    /// Runs `GET {uri}` through the real Hub router and returns the status.
    async fn get_status(state: HubState, uri: &str) -> StatusCode {
        let app = crate::routes::router::router(true).with_state(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header(AUTHORIZATION, "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        response.status()
    }

    #[tokio::test]
    async fn deep_tree_tail_cannot_bypass_cross_tenant_binding() {
        // F-104 exploit: a Read token for `alice/own` must NOT read `bob/own`
        // by appending a deep tail whose last three segments look like a
        // `[type, ns, repo]` triple (`datasets/alice/own`). The extractor must
        // bind to the route's `bob/own`, so the cross-tenant read is denied.
        let (_td, state) = scoped_state("alice", "own");
        let status = get_status(state, "/api/models/bob/own/tree/main/datasets/alice/own").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn deep_tree_tail_legit_owner_read_succeeds() {
        // F-104 regression: a Read token for `bob/own` reading `bob/own` with a
        // deep tail must NOT be spuriously 403'd by the tail being misparsed as
        // a different repository.
        let (_td, state) = scoped_state("bob", "own");
        let status = get_status(state, "/api/models/bob/own/tree/main/datasets/foo/bar").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn repo_named_like_repo_type_word_still_binds() {
        // F-37 regression: a repository whose name collides with a repo-type
        // word (`models`) must still bind to `alice/models` — both when the
        // path ends at the repository and when a deep tail follows.
        let (_td, state) = scoped_state("alice", "models");
        let status = get_status(state, "/api/models/alice/models").await;
        assert_eq!(status, StatusCode::OK);
        let (_td, state) = scoped_state("alice", "models");
        let status = get_status(state, "/api/models/alice/models/tree/main/datasets/foo/bar").await;
        assert_eq!(status, StatusCode::OK);
    }
}
