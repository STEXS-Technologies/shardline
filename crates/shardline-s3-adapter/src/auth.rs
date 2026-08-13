use axum::http::{HeaderMap, header::AUTHORIZATION};
use shardline_protocol::RepositoryScope;

use crate::{error::S3Error, key::decode_bucket, protocol_support::QueryMap};

/// The `x-amz-security-token` header name (not exposed by the `http` crate).
const X_AMZ_SECURITY_TOKEN_HEADER: &str = "x-amz-security-token";

/// Extracts the S3 access key — the Shardline bearer token — from request
/// headers.
///
/// The S3 frontend bridges SigV4 clients: the client's `access_key` **is** the
/// Shardline bearer token, so the SigV4 signature itself is never verified.
/// Accepted forms:
///
/// - `Authorization: AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/<service>/aws4_request`
/// - `Authorization: Bearer <token>`
/// - `x-amz-security-token: <token>` (fallback)
///
/// # Examples
///
/// ```
/// use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
/// use shardline_s3_adapter::extract_access_key;
///
/// let mut headers = HeaderMap::new();
/// headers.insert(
///     AUTHORIZATION,
///     HeaderValue::from_static(
///         "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, \
///          SignedHeaders=host;x-amz-date, Signature=abc123",
///     ),
/// );
/// assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
/// ```
#[must_use]
pub fn extract_access_key(headers: &HeaderMap) -> Option<&str> {
    let authorization = headers.get(AUTHORIZATION);
    if let Some(authorization) = authorization {
        let value = authorization.to_str().ok()?;
        if let Some(token) = value.strip_prefix("Bearer ") {
            return Some(token);
        }
        if let Some(access_key) = extract_sigv4_access_key(value) {
            return Some(access_key);
        }
    }
    headers.get(X_AMZ_SECURITY_TOKEN_HEADER)?.to_str().ok()
}

/// Extracts the access-key component from a SigV4 `Authorization` header value.
fn extract_sigv4_access_key(value: &str) -> Option<&str> {
    let credential = value.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let credential = credential.split(',').next()?.trim_start();
    let access_key = credential.strip_prefix("Credential=")?.split('/').next()?;
    if access_key.is_empty() {
        None
    } else {
        Some(access_key)
    }
}

/// Extracts the S3 access key from the query string (presigned-URL form).
///
/// Presigned requests carry
/// `X-Amz-Credential=<access_key>/<date>/<region>/<service>/aws4_request`
/// as a query parameter. Values are expected to be percent-decoded (the
/// handler lane's query extraction decodes them).
///
/// # Examples
///
/// ```
/// use shardline_s3_adapter::extract_access_key_from_query;
///
/// let query = vec![
///     ("X-Amz-Credential".to_owned(), "AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request".to_owned()),
///     ("X-Amz-Signature".to_owned(), "abc123".to_owned()),
/// ];
/// assert_eq!(extract_access_key_from_query(&query), Some("AKIDEXAMPLE"));
/// ```
#[must_use]
pub fn extract_access_key_from_query(query: &QueryMap) -> Option<&str> {
    query.iter().find_map(|(name, value)| {
        if name == "X-Amz-Credential" {
            value.split('/').next().filter(|key| !key.is_empty())
        } else {
            None
        }
    })
}

/// Requires the bucket to bind exactly to the token's repository scope.
///
/// Every S3 request must carry a bearer token whose `owner`/`name` match the
/// decoded `{owner}.{name}` bucket (the C1 repo-binding model). An
/// undecodable bucket is `404 NoSuchBucket`; a claims mismatch (or missing
/// claims) is `403 AccessDenied`.
///
/// # Errors
///
/// Returns [`S3Error::no_such_bucket`] when the bucket cannot be decoded, and
/// [`S3Error::access_denied`] when the claims are absent or do not match the
/// bucket.
pub fn require_s3_bucket_binding(
    claims: Option<&RepositoryScope>,
    bucket: &str,
) -> Result<(), S3Error> {
    let (owner, name) = decode_bucket(bucket).map_err(|_error| S3Error::no_such_bucket(bucket))?;
    let claims = claims.ok_or_else(S3Error::access_denied)?;
    if claims.owner() == owner.as_str() && claims.name() == name.as_str() {
        Ok(())
    } else {
        Err(S3Error::access_denied())
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::*;

    fn header_map(entries: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in entries {
            headers.insert(*name, HeaderValue::from_static(value));
        }
        headers
    }

    fn scope(owner: &str, name: &str) -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, owner, name, None).unwrap()
    }

    #[test]
    fn sigv4_header_extracts_access_key_from_full_credential() {
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=abc123",
        )]);
        assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
    }

    #[test]
    fn sigv4_header_extracts_access_key_from_bare_credential() {
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request",
        )]);
        assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
    }

    #[test]
    fn missing_authorization_header_returns_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_access_key(&headers), None);
    }

    #[test]
    fn malformed_sigv4_header_returns_none() {
        // Wrong scheme.
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc",
        )]);
        assert_eq!(extract_access_key(&headers), None);
        // Scheme present but not the SigV4 scheme.
        let headers = header_map(&[(AUTHORIZATION.as_str(), "Basic dXNlcjpwYXNz")]);
        assert_eq!(extract_access_key(&headers), None);
        // Empty access key in the credential scope.
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=/20150830/us-east-1/s3/aws4_request",
        )]);
        assert_eq!(extract_access_key(&headers), None);
    }

    #[test]
    fn bearer_header_is_returned_as_token() {
        let headers = header_map(&[(AUTHORIZATION.as_str(), "Bearer shardline-token-abc")]);
        assert_eq!(extract_access_key(&headers), Some("shardline-token-abc"));
    }

    #[test]
    fn x_amz_security_token_header_is_accepted() {
        let headers = header_map(&[("x-amz-security-token", "shardline-token-xyz")]);
        assert_eq!(extract_access_key(&headers), Some("shardline-token-xyz"));
    }

    #[test]
    fn query_credential_form_extracts_access_key() {
        let query = vec![
            (
                "X-Amz-Credential".to_owned(),
                "AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request".to_owned(),
            ),
            ("X-Amz-Signature".to_owned(), "abc123".to_owned()),
        ];
        assert_eq!(extract_access_key_from_query(&query), Some("AKIDEXAMPLE"));
    }

    #[test]
    fn query_credential_form_absent_returns_none() {
        let query = vec![("prefix".to_owned(), "dir/".to_owned())];
        assert_eq!(extract_access_key_from_query(&query), None);
        // Empty credential.
        let query = vec![("X-Amz-Credential".to_owned(), String::new())];
        assert_eq!(extract_access_key_from_query(&query), None);
    }

    #[test]
    fn bucket_binding_matches_exact_scope() {
        let claims = scope("acme", "models");
        assert_eq!(
            require_s3_bucket_binding(Some(&claims), "acme.models"),
            Ok(())
        );
    }

    #[test]
    fn bucket_binding_rejects_owner_mismatch() {
        let claims = scope("acme", "models");
        let error = require_s3_bucket_binding(Some(&claims), "other.models").unwrap_err();
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, 403);
    }

    #[test]
    fn bucket_binding_rejects_name_mismatch() {
        let claims = scope("acme", "models");
        let error = require_s3_bucket_binding(Some(&claims), "acme.datasets").unwrap_err();
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, 403);
    }

    #[test]
    fn bucket_binding_rejects_missing_claims() {
        let error = require_s3_bucket_binding(None, "acme.models").unwrap_err();
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, 403);
    }

    #[test]
    fn bucket_binding_rejects_undecodable_bucket() {
        let claims = scope("acme", "models");
        let error = require_s3_bucket_binding(Some(&claims), "not-a-bucket").unwrap_err();
        assert_eq!(error.code, "NoSuchBucket");
        assert_eq!(error.status, 404);
        // Uppercase buckets are undecodable too.
        let error = require_s3_bucket_binding(Some(&claims), "Acme.models").unwrap_err();
        assert_eq!(error.code, "NoSuchBucket");
    }
}
