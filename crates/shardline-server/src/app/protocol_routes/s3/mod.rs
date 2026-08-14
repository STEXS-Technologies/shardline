//! S3 object-data routes (`PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`) and
//! bucket stubs (`/{bucket}`) for the S3 frontend (Lane 3).
//!
//! Routing is `/{bucket}/{*key}` where `{bucket}` is the single dotted segment
//! `{owner}.{name}` of the bearer token's `RepositoryScope` and `{*key}` is the
//! arbitrary-depth S3 object key. Every handler carries the [`S3Repository`]
//! axum extractor, which authenticates with the SigV4→bearer bridge
//! ([`authorize_s3`]), binds the bucket to the token claims, and mints the
//! typed [`AuthorizedRepository`] capability that storage entry points require;
//! handlers then dispatch on the query sub-resources.

pub(super) mod aws_chunked;
pub(super) mod bucket;
pub(super) mod listing;
pub(super) mod multipart;
pub(super) mod object;

#[cfg(test)]
mod poc_audit;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex, Weak},
};

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, HeaderValue, Method, Uri, header::AUTHORIZATION, request::Parts},
};
use shardline_protocol::TokenScope;
use shardline_s3_adapter::{
    QueryMap, S3Error, encode_bucket, extract_access_key, require_s3_bucket_binding, s3_object_key,
};
use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectKey;

use crate::{ServerError, app::AppState, auth::AuthContext, protocol_support::scope_namespace};

/// Serializes overwrite operations (`PutObject` and multipart completion)
/// per storage object key.
///
/// The overwrite is upload-then-swap: the new body is streamed to a new record
/// version first, then the index row is swapped and any stale direct object
/// dropped. The per-key lock prevents two concurrent overwrites of the same
/// key from interleaving their swaps.
///
/// The map holds **weak** values: while any caller holds a guard, its strong
/// [`Arc`] keeps the entry's weak handle alive so concurrent overwrites of the
/// same key still serialize on the SAME mutex; once the last guard drops the
/// entry dies and is evicted lazily on the next acquire (or opportunistically
/// when a fresh lock is inserted). This bounds the map by the number of keys
/// with an upload in flight instead of the number of distinct keys ever seen
/// (F-9: unique-key PUTs must not leak an entry each).
static S3_OBJECT_UPLOAD_LOCKS: LazyLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Returns the per-key upload lock for an object, creating it on first use.
///
/// The returned strong [`Arc`] keeps the map entry alive for as long as the
/// caller holds it (and its guard), so concurrent acquires for the same key
/// return the SAME mutex. Once the last guard drops, the map's weak handle
/// goes dead and is cleaned up on the next acquire for that key.
pub(super) fn acquire_object_upload_lock(object_key: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut map = S3_OBJECT_UPLOAD_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Fast path: a live weak handle exists (a guard is still being held for
    // this key), so hand out the same strong Arc to preserve serialization.
    if let Some(live) = map.get(object_key).and_then(Weak::upgrade) {
        return live;
    }
    // No live handle: the previous entry (if any) has no holders left. Drop
    // any other dead entries so the map cannot grow with finished keys (F-9),
    // then install a fresh mutex and return its strong Arc — the only strong
    // reference until a caller takes a guard.
    map.retain(|_key, weak| weak.upgrade().is_some());
    let fresh = Arc::new(tokio::sync::Mutex::new(()));
    map.insert(object_key.to_owned(), Arc::downgrade(&fresh));
    fresh
}

/// Returns the number of map entries whose strong lock is still alive (i.e.
/// held by at least one guard). Test-only: asserts the map is bounded by
/// in-flight uploads rather than by the number of distinct keys ever seen.
#[cfg(test)]
pub(super) fn live_upload_lock_count() -> usize {
    let map = S3_OBJECT_UPLOAD_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.values().filter(|weak| weak.upgrade().is_some()).count()
}

pub(crate) use bucket::{
    s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket, s3_list_buckets,
    s3_post_bucket,
};
pub(crate) use object::{
    s3_delete_object, s3_get_object, s3_head_object, s3_post_object, s3_put_object,
};

/// Maximum accepted S3 query-string length (reuses the protocol query budget).
use crate::app::MAX_PROTOCOL_QUERY_BYTES;

/// The S3 XML content type.
const S3_XML_CONTENT_TYPE: &str = "application/xml";

/// Authenticates an S3 request by bridging the SigV4 access key (or a plain
/// bearer token) to the Shardline token verifier.
///
/// The client's access key **is** the bearer token, so it is extracted from the
/// `Authorization: AWS4-HMAC-SHA256 Credential=...` header (or `Bearer`/`x-amz-security-token`)
/// and verified against the configured auth provider. With no auth provider
/// configured the request is allowed through (strict mode fails closed).
///
/// # Errors
///
/// Returns [`S3Error::access_denied`] when the access key is absent or the
/// token fails verification or scope checks.
pub(super) fn authorize_s3(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, S3Error> {
    if let Some(auth) = &state.auth {
        let access_key = extract_access_key(headers).ok_or_else(S3Error::access_denied)?;
        let mut bearer_headers = HeaderMap::new();
        bearer_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {access_key}"))
                .map_err(|_error| S3Error::access_denied())?,
        );
        let context = auth
            .authorize(&bearer_headers, required_scope)
            .map_err(S3Error::from)?;
        return Ok(Some(context));
    }

    // No auth provider configured.
    if state.config.deployment_mode() == crate::config::DeploymentMode::Strict {
        return Err(S3Error::internal());
    }
    Ok(None)
}

/// A typed, repository-scoped S3 authorization capability extracted by axum.
///
/// `S3Repository` is the **only** way an S3 handler may obtain an
/// [`AuthorizedRepository`]: its [`FromRequestParts`] implementation reproduces
/// the exact S3 authorization + binding chain — SigV4 access key / bearer
/// bridge → token verification + scope ([`authorize_s3`]) → URI-bucket ↔ claims
/// binding ([`require_s3_bucket_binding`]) — in the same order as the
/// pre-refactor `authorize_s3` + `require_s3_object_context` calls, and then
/// mints the capability from the already-verified context. A handler that does
/// not carry this extractor cannot reach repository-scoped storage at all.
pub struct S3Repository {
    pub(crate) inner: AuthorizedRepository,
}

impl S3Repository {
    /// The verified, scope-checked capability backing this extractor.
    #[must_use]
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.inner
    }

    /// Reproduces the S3 authorization + binding chain and mints a capability
    /// with the given required scope.
    ///
    /// 1. SigV4 access key → bearer bridge, token verification, scope check
    ///    ([`authorize_s3`] unchanged: permissive deployments with no auth
    ///    provider yield `Ok(None)` unless strict mode fails closed).
    /// 2. When the URI carries a bucket (every route but the service-level
    ///    `ListBuckets`), bind it to the token claims BEFORE minting: malformed
    ///    bucket → `404 NoSuchBucket`, claims mismatch or missing claims →
    ///    `403 AccessDenied`.
    /// 3. Mint the capability: `Some(context)` (already verified by the auth
    ///    layer) → [`AuthorizedRepository::from_verified_context`] (no token is
    ///    re-verified; the scope gate is re-applied idempotently); `None`
    ///    (permissive) → [`AuthorizedRepository::anonymous_full_access`], whose
    ///    `None` namespace resolves to the global namespace exactly like
    ///    `scope_namespace(None)`.
    fn authorize(
        state: &Arc<AppState>,
        headers: &HeaderMap,
        uri: &Uri,
        required_scope: TokenScope,
    ) -> Result<Self, S3Error> {
        let auth = authorize_s3(state, headers, required_scope)?;

        if let Some(bucket) = bucket_from_uri(uri) {
            let claims = auth.as_ref().map(|context| context.claims().repository());
            require_s3_bucket_binding(claims, &bucket)?;
        }

        let inner = match auth {
            Some(context) => {
                let core_context =
                    shardline_server_core::AuthContext::new(context.claims().clone());
                AuthorizedRepository::from_verified_context(core_context, required_scope)
                    .map_err(|error| S3Error::from(ServerError::from(error)))?
            }
            None => AuthorizedRepository::anonymous_full_access(),
        };
        Ok(Self { inner })
    }
}

impl FromRequestParts<Arc<AppState>> for S3Repository {
    type Rejection = S3Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        // The required scope follows every registered handler's per-method
        // mapping (GET/HEAD → Read, PUT/POST/DELETE → Write), so each handler
        // mints with exactly the scope its pre-refactor `authorize_s3` call used.
        let required_scope = match parts.method {
            Method::GET | Method::HEAD => TokenScope::Read,
            Method::PUT | Method::POST | Method::DELETE => TokenScope::Write,
            _ => return Err(S3Error::access_denied()),
        };
        Self::authorize(state, &parts.headers, &parts.uri, required_scope)
    }
}

/// Parses the `{owner}.{name}` bucket segment from a request URI path.
///
/// The bucket is the first path segment, percent-decoded exactly like axum's
/// `Path` extractor. Returns `None` for the service-level path `/`
/// (`ListBuckets`), which carries no bucket and therefore no bucket binding.
fn bucket_from_uri(uri: &Uri) -> Option<String> {
    let segment = uri.path().trim_start_matches('/').split('/').next()?;
    if segment.is_empty() {
        return None;
    }
    let decoded = percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_error| segment.to_owned());
    Some(decoded)
}

/// The scope namespace, storage object key, and client key for one S3 request.
pub(super) struct S3ObjectContext<'ctx> {
    /// The bound capability this context was derived from.
    pub(super) auth: &'ctx AuthorizedRepository,
    /// The bucket name (`{owner}.{name}`), derived from the bound capability.
    pub(super) bucket: String,
    /// sha256 repository-scope namespace keying the listing index rows.
    pub(super) scope_namespace: String,
    /// The client-facing S3 object key (no `protocols/s3/` prefix).
    pub(super) key: String,
    /// The storage object key (`protocols/s3/{scope_namespace}/{key}`).
    pub(super) object_key: ObjectKey,
}

/// Derives the storage object key for one S3 object request from the bound
/// capability.
///
/// The bucket↔claims binding already happened in the [`S3Repository`]
/// extractor, so the context's bucket and scope namespace are derived from the
/// capability itself (equal to the URI bucket by construction). Only the
/// key-level validation remains here — unchanged semantics:
/// [`S3Error::no_such_key`] for an invalid key.
///
/// # Errors
///
/// Returns [`S3Error::access_denied`] when the capability carries no repository
/// (unreachable in practice: permissive capabilities never survive the
/// extractor's bucket binding), and [`S3Error::no_such_key`] when the object
/// key is invalid.
pub(super) fn require_s3_object_context<'ctx>(
    auth: &'ctx AuthorizedRepository,
    key: &str,
) -> Result<S3ObjectContext<'ctx>, S3Error> {
    let bucket = auth
        .owner()
        .zip(auth.name())
        .map(|(owner, name)| encode_bucket(owner, name))
        .ok_or_else(S3Error::access_denied)?;
    let scope_namespace = scope_namespace(auth.namespace());
    let object_key =
        s3_object_key(&scope_namespace, key).map_err(|_error| S3Error::no_such_key(key))?;
    Ok(S3ObjectContext {
        auth,
        bucket,
        scope_namespace,
        key: key.to_owned(),
        object_key,
    })
}

/// Parses the ordered, percent-decoded query pairs of a request URI.
///
/// # Errors
///
/// Returns an internal [`S3Error`] when the query exceeds the bounded metadata
/// parser budget.
pub(super) fn parse_s3_query(uri: &Uri) -> Result<QueryMap, S3Error> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };
    if query.len() > MAX_PROTOCOL_QUERY_BYTES {
        return Err(S3Error::from(ServerError::RequestQueryTooLarge));
    }
    Ok(url::form_urlencoded::parse(query.as_bytes())
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect())
}

/// Formats unix seconds as an RFC 7231 IMF-fixdate HTTP date (`GMT`).
///
/// Falls back to the Unix epoch when the timestamp is out of the representable
/// `chrono` range (a timestamp of `0` yields the epoch date).
#[must_use]
pub(super) fn format_http_date(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|date_time| date_time.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_owned())
}

/// Returns `true` when the query contains any recognized S3 sub-resource.
#[must_use]
pub(super) fn has_sub_resource(query: &QueryMap) -> bool {
    !shardline_s3_adapter::classify(query).is_empty()
}

/// The S3 XML content-type header value used by error and stub responses.
#[must_use]
pub(super) const fn s3_xml_content_type() -> &'static str {
    S3_XML_CONTENT_TYPE
}
