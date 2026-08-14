use axum::http::{HeaderMap, header::AUTHORIZATION};
use shardline_protocol::RepositoryScope;

use crate::{error::S3Error, key::decode_bucket, protocol_support::QueryMap};

/// The `x-amz-security-token` header name (not exposed by the `http` crate).
const X_AMZ_SECURITY_TOKEN_HEADER: &str = "x-amz-security-token";

/// The typed `Authorization` header scheme.
///
/// [`AuthScheme::try_from`] is the single typed choke point between the raw
/// header value and the scheme; callers match the enum, never the string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthScheme {
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// `Authorization: AWS4-HMAC-SHA256 Credential=<key>/…`.
    SigV4,
}

impl TryFrom<&str> for AuthScheme {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = value.as_bytes();
        if bytes.get(.."Bearer ".len()) == Some(b"Bearer ") {
            Ok(Self::Bearer)
        } else if bytes.get(.."AWS4-HMAC-SHA256 ".len()) == Some(b"AWS4-HMAC-SHA256 ") {
            Ok(Self::SigV4)
        } else {
            Err(())
        }
    }
}

impl AuthScheme {
    /// The byte length of the scheme prefix, for slicing the value past it.
    #[must_use]
    pub const fn prefix_len(self) -> usize {
        match self {
            Self::Bearer => "Bearer ".len(),
            Self::SigV4 => "AWS4-HMAC-SHA256 ".len(),
        }
    }
}

/// The five `/`-separated segments of a SigV4 `Credential` scope.
///
/// Parsing is deliberately lenient: only the access key is meaningful to the
/// frontend (the signature is never verified), so missing/extra trailing
/// segments are tolerated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialScope<'value> {
    /// The access key — the Shardline bearer token.
    pub access_key: &'value str,
    /// The credential date scope segment.
    pub date: &'value str,
    /// The credential region scope segment.
    pub region: &'value str,
    /// The credential service scope segment.
    pub service: &'value str,
    /// The credential terminator segment (`aws4_request`).
    pub terminator: &'value str,
}

impl<'value> CredentialScope<'value> {
    /// Parses a `/`-separated credential scope. Missing trailing segments
    /// become empty; extra segments are ignored (the access key is the only
    /// meaningful part).
    #[must_use]
    pub fn parse(value: &'value str) -> Self {
        let mut parts = value.split('/');
        let access_key = parts.next().unwrap_or("");
        let date = parts.next().unwrap_or("");
        let region = parts.next().unwrap_or("");
        let service = parts.next().unwrap_or("");
        let terminator = parts.next().unwrap_or("");
        Self {
            access_key,
            date,
            region,
            service,
            terminator,
        }
    }

    /// Parses the `Credential=<scope>` field of a SigV4 `Authorization` header.
    ///
    /// Returns `None` when the field prefix is absent or the access key is
    /// empty.
    #[must_use]
    pub fn parse_credential_field(field: &'value str) -> Option<Self> {
        let scope = field.strip_prefix("Credential=")?;
        let scope = Self::parse(scope);
        if scope.access_key.is_empty() {
            None
        } else {
            Some(scope)
        }
    }
}

/// The presigned-URL `X-Amz-Credential` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCredential<'value>(CredentialScope<'value>);

impl<'value> QueryCredential<'value> {
    /// Parses the `X-Amz-Credential` value into its credential scope.
    ///
    /// Returns `None` when the access key is empty.
    #[must_use]
    pub fn parse(value: &'value str) -> Option<Self> {
        let scope = CredentialScope::parse(value);
        if scope.access_key.is_empty() {
            None
        } else {
            Some(Self(scope))
        }
    }

    /// Returns the parsed access key.
    #[must_use]
    pub const fn access_key(&self) -> &'value str {
        self.0.access_key
    }

    /// Returns the parsed credential scope.
    #[must_use]
    pub const fn scope(&self) -> &CredentialScope<'value> {
        &self.0
    }
}

/// The `X-Amz-Credential` query-parameter name, matched case-insensitively.
const fn is_amz_credential_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("X-Amz-Credential")
}

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
        let authorization_token = match AuthScheme::try_from(value) {
            Ok(AuthScheme::Bearer) => value.get(AuthScheme::Bearer.prefix_len()..),
            Ok(AuthScheme::SigV4) => extract_sigv4_access_key(value),
            Err(()) => None,
        };
        if let Some(token) = authorization_token {
            return Some(token);
        }
    }
    headers.get(X_AMZ_SECURITY_TOKEN_HEADER)?.to_str().ok()
}

/// Extracts the access-key component from a SigV4 `Authorization` header value.
fn extract_sigv4_access_key(value: &str) -> Option<&str> {
    let credential = value.get(AuthScheme::SigV4.prefix_len()..)?;
    let credential = credential.split(',').next()?.trim_start();
    let scope = CredentialScope::parse_credential_field(credential)?;
    Some(scope.access_key)
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
        if is_amz_credential_name(name) {
            QueryCredential::parse(value).map(|credential| credential.access_key())
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

    #[test]
    fn sigv4_header_ignores_extra_commas_and_malformed_dates() {
        // Extra commas / malformed date segments after the access key are
        // irrelevant — only the first Credential path segment is the key.
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/,,,/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=abc",
        )]);
        assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/not-a-date/us-east-1/s3/aws4_request",
        )]);
        assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
        // A trailing credential scope with extra slash segments is fine too.
        let headers = header_map(&[(
            AUTHORIZATION.as_str(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260813//s3///aws4_request",
        )]);
        assert_eq!(extract_access_key(&headers), Some("AKIDEXAMPLE"));
    }

    #[test]
    fn duplicate_authorization_headers_first_wins() {
        let mut headers = HeaderMap::new();
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer first-token"),
        );
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer second-token"),
        );
        assert_eq!(extract_access_key(&headers), Some("first-token"));
    }

    #[test]
    fn bearer_with_trailing_space_is_returned_raw() {
        // The adapter returns the raw value; the server-side token verifier
        // rejects tokens containing whitespace.
        let headers = header_map(&[(AUTHORIZATION.as_str(), "Bearer token ")]);
        assert_eq!(extract_access_key(&headers), Some("token "));
    }

    #[test]
    fn security_token_is_used_when_credential_is_absent_or_malformed() {
        let headers = header_map(&[("x-amz-security-token", "token-only")]);
        assert_eq!(extract_access_key(&headers), Some("token-only"));
        // A malformed SigV4 header does not shadow the security-token fallback.
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("AWS4-HMAC-SHA256 malformed-no-credential"),
        );
        headers.insert(
            "x-amz-security-token",
            HeaderValue::from_static("fallback-token"),
        );
        assert_eq!(extract_access_key(&headers), Some("fallback-token"));
    }

    #[test]
    fn header_and_query_credentials_are_deterministic_and_independent() {
        // The frontend handlers authenticate from the Authorization header
        // only; the query form is a separate function. When both are present
        // the header function reads the header and the query function reads
        // the query — each deterministic in isolation.
        let headers = header_map(&[(AUTHORIZATION.as_str(), "Bearer header-token")]);
        let query = vec![(
            "X-Amz-Credential".to_owned(),
            "query-token/20260813/us-east-1/s3/aws4_request".to_owned(),
        )];
        assert_eq!(extract_access_key(&headers), Some("header-token"));
        assert_eq!(extract_access_key_from_query(&query), Some("query-token"));
        // Empty credential in the query → None.
        let empty = vec![("X-Amz-Credential".to_owned(), String::new())];
        assert_eq!(extract_access_key_from_query(&empty), None);
    }

    #[test]
    fn auth_scheme_is_typed() {
        assert_eq!(AuthScheme::try_from("Bearer token"), Ok(AuthScheme::Bearer));
        assert_eq!(
            AuthScheme::try_from("AWS4-HMAC-SHA256 Credential=k/d/r/s/a"),
            Ok(AuthScheme::SigV4)
        );
        assert!(AuthScheme::try_from("Basic dXNlcg==").is_err());
        assert_eq!(AuthScheme::Bearer.prefix_len(), "Bearer ".len());
        assert_eq!(AuthScheme::SigV4.prefix_len(), "AWS4-HMAC-SHA256 ".len());
    }

    #[test]
    fn credential_scope_parses_typed_segments() {
        let scope = CredentialScope::parse("AKID/20260813/us-east-1/s3/aws4_request");
        assert_eq!(scope.access_key, "AKID");
        assert_eq!(scope.date, "20260813");
        assert_eq!(scope.region, "us-east-1");
        assert_eq!(scope.service, "s3");
        assert_eq!(scope.terminator, "aws4_request");
        // Lenient: extra segments are ignored, missing ones become empty.
        let scope = CredentialScope::parse("AKID/20260813//s3///aws4_request");
        assert_eq!(scope.access_key, "AKID");
        let scope = CredentialScope::parse("AKID");
        assert_eq!(scope.date, "");
        assert_eq!(scope.terminator, "");
    }

    #[test]
    fn credential_field_requires_the_prefix() {
        let scope = CredentialScope::parse_credential_field("Credential=AKID/20260813/s3/aws4");
        assert_eq!(scope.unwrap().access_key, "AKID");
        // A field that does not start with `Credential=` is not a scope.
        assert!(CredentialScope::parse_credential_field("SignedHeaders=host").is_none());
        assert!(CredentialScope::parse_credential_field("Credential=").is_none());
    }

    #[test]
    fn query_credential_is_typed_and_case_insensitive() {
        let query = vec![
            (
                "x-amz-credential".to_owned(),
                "AKID/20260813/us-east-1/s3/aws4_request".to_owned(),
            ),
            ("X-Amz-Signature".to_owned(), "abc".to_owned()),
        ];
        assert_eq!(extract_access_key_from_query(&query), Some("AKID"));
        let credential = QueryCredential::parse("AKID/20260813/us-east-1/s3/aws4_request").unwrap();
        assert_eq!(credential.access_key(), "AKID");
        assert_eq!(credential.scope().region, "us-east-1");
        assert!(QueryCredential::parse("").is_none());
    }
}
