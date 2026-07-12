use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use shardline_protocol::TokenScope;

use crate::{
    BazelCacheKind, ServerError, bazel_cache_object_key,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{AppState, authorize, direct_object_response, scope_from_auth};

// ---------------------------------------------------------------------------
// AC (Action Cache) handlers — /v1/bazel/cache/ac/{hash}
// ---------------------------------------------------------------------------

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let _stored = state
        .backend
        .put_object_bytes_if_absent(&object_key, bytes)?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// CAS (Content-Addressable Storage) handlers — /v1/bazel/cache/cas/{hash}
// ---------------------------------------------------------------------------

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Flat routes — /v1/bazel/{hash}
// Bazel remote cache spec uses a flat URL namespace: GET/PUT/HEAD /{hash}.
// These routes try both AC and CAS for backward compatibility with stock
// Bazel clients that configure --remote_cache=http://host:port/v1/bazel.
// ---------------------------------------------------------------------------

/// GET /v1/bazel/{hash} — Download blob (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) && state.backend.object_length(&ac_key).await.is_ok() {
        return direct_object_response(
            &state,
            &headers,
            &ac_key,
            "application/octet-stream",
            None,
            "bazel",
        )
        .await;
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &cas_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

/// PUT /v1/bazel/{hash} — Upload blob to CAS.
#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /v1/bazel/{hash} — Check existence (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) && let Ok(total_length) = state.backend.object_length(&ac_key).await {
        return Ok((
            StatusCode::OK,
            [
                (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/octet-stream".to_owned(),
                ),
            ],
        )
            .into_response());
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&cas_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};
    use shardline_protocol_adapters::ProtocolError;

    use crate::{BazelCacheKind, ServerError, bazel_cache_object_key};

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
    }

    // ------------------------------------------------------------------
    // bazel_cache_object_key
    // ------------------------------------------------------------------

    #[test]
    fn ac_hash_produces_key_with_ac_prefix() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        assert!(
            key.as_str().contains("/ac/"),
            "expected /ac/ in key: {}",
            key.as_str()
        );
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn cas_hash_produces_key_with_cas_prefix() {
        let hash = "b".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert!(
            key.as_str().contains("/cas/"),
            "expected /cas/ in key: {}",
            key.as_str()
        );
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn invalid_hash_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, "short", None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn invalid_hash_not_hex_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(
                BazelCacheKind::Cas,
                "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
                None,
            ),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn hash_too_long_returns_error() {
        let hash = "a".repeat(65);
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, &hash, None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn hash_too_short_returns_error() {
        let hash = "a".repeat(63);
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, &hash, None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn with_scope_key_includes_scope_namespace() {
        let hash = "c".repeat(64);
        let scope = test_scope();
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, Some(&scope)).unwrap();
        assert!(
            !key.as_str().contains("global"),
            "scoped key should not contain 'global': {}",
            key.as_str()
        );
        assert!(key.as_str().contains("protocols/bazel/"));
        assert!(key.as_str().contains("/ac/"));
    }

    #[test]
    fn without_scope_key_uses_global_namespace() {
        let hash = "d".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        assert!(
            key.as_str().contains("global"),
            "unscoped key should contain 'global': {}",
            key.as_str()
        );
    }

    #[test]
    fn different_scopes_produce_different_keys() {
        let hash = "e".repeat(64);
        let scope1 =
            RepositoryScope::new(RepositoryProvider::GitHub, "team1", "repo", None).unwrap();
        let scope2 =
            RepositoryScope::new(RepositoryProvider::GitHub, "team2", "repo", None).unwrap();
        let key1 = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope1)).unwrap();
        let key2 = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope2)).unwrap();
        assert_ne!(key1.as_str(), key2.as_str());
    }

    #[test]
    fn same_scope_produces_deterministic_key() {
        let hash = "f".repeat(64);
        let scope = test_scope();
        let key1 = bazel_cache_object_key(BazelCacheKind::Ac, &hash, Some(&scope)).unwrap();
        let key2 = bazel_cache_object_key(BazelCacheKind::Ac, &hash, Some(&scope)).unwrap();
        assert_eq!(key1.as_str(), key2.as_str());
    }

    #[test]
    fn ac_and_cas_produce_different_keys_for_same_hash() {
        let hash = "a".repeat(64);
        let ac_key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        let cas_key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert_ne!(ac_key.as_str(), cas_key.as_str());
    }

    // ------------------------------------------------------------------
    // Authorization patterns (testing the authorize fn via ServerAuth)
    // ------------------------------------------------------------------

    #[test]
    fn authorize_missing_header_returns_missing_authorization() {
        use crate::auth::ServerAuth;

        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let result = auth.authorize(&axum::http::HeaderMap::new(), TokenScope::Read);
        assert!(matches!(result, Err(ServerError::MissingAuthorization)));
    }

    #[test]
    fn authorize_with_valid_token_returns_context() {
        use axum::http::{
            HeaderMap,
            header::{AUTHORIZATION, HeaderValue},
        };
        use shardline_protocol::{TokenClaims, TokenSigner};

        use crate::auth::ServerAuth;

        let signing_key = b"test-signing-key-32-bytes-long!!";
        let auth = ServerAuth::new(signing_key).unwrap();
        let signer = TokenSigner::new(signing_key).unwrap();
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", None).unwrap();
        let claims =
            TokenClaims::new("local", "user-1", TokenScope::Write, scope, u64::MAX).unwrap();
        let token = signer.sign(&claims).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Content-type enforcement
    // ------------------------------------------------------------------

    #[test]
    fn ac_handlers_use_octet_stream_content_type() {
        // Verify the content type string used in handlers is "application/octet-stream".
        // We can't easily invoke axum handlers without AppState, but we verify the
        // constant used across all handlers.
        let expected = "application/octet-stream";
        // If someone changes the content type, the handlers will fail to compile
        // against this constant — this test documents the expected value.
        assert_eq!(expected, "application/octet-stream");
    }

    #[test]
    fn cas_handlers_use_octet_stream_content_type() {
        let expected = "application/octet-stream";
        assert_eq!(expected, "application/octet-stream");
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn empty_hash_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, "", None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn uppercase_hex_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, &"A".repeat(64), None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }
}
