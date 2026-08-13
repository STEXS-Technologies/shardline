use axum::http::{
    HeaderMap,
    header::{IF_MATCH, IF_NONE_MATCH},
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::error::S3Error;

/// An ordered query-parameter list.
///
/// Values are expected to be percent-decoded; the handler lane's query
/// extraction (for example axum's `Query` extractor) decodes them.
pub type QueryMap = Vec<(String, String)>;

/// A recognized S3 sub-resource (the `?subresource` query dispatch set).
///
/// The `Acl`…`Encryption` variants and [`S3SubResource::Other`] are
/// out-of-scope operations that the handler lane maps to
/// `501 NotImplemented` (per `docs/S3_FRONTEND.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3SubResource {
    /// `?uploads` — `CreateMultipartUpload`.
    Uploads,
    /// `?uploadId=<id>` — part upload / completion dispatch.
    UploadId(String),
    /// `?partNumber=<n>` — a specific part within an upload.
    PartNumber(u32),
    /// `?list-type=2` — `ListObjectsV2`.
    ListObjects,
    /// `?delete=` — `DeleteObjects` (batch delete).
    DeleteObjects,
    /// `?location` — `GetBucketLocation` stub.
    Location,
    /// `?acl` — out of scope (`NotImplemented`).
    Acl,
    /// `?policy` — out of scope (`NotImplemented`).
    Policy,
    /// `?lifecycle` — out of scope (`NotImplemented`).
    Lifecycle,
    /// `?versioning` — out of scope (`NotImplemented`).
    Versioning,
    /// `?cors` — out of scope (`NotImplemented`).
    Cors,
    /// `?notification` — out of scope (`NotImplemented`).
    Notification,
    /// `?tagging` — out of scope (`NotImplemented`).
    Tagging,
    /// `?encryption` — out of scope (`NotImplemented`).
    Encryption,
    /// Any other recognized-but-unsupported sub-resource (for example
    /// `restore`, `select`, `torrent`, `legal-hold`, `retention`, `attributes`,
    /// `replication`, `website`, `versionId`, `versions`, `logging`,
    /// `requestPayment`, `accelerate`, `object-lock`, `list-type=1`, or a
    /// malformed `partNumber`) — `NotImplemented` at the handler level.
    Other,
}

/// A typed S3 query-parameter name (the `?subresource` dispatch set plus the
/// recognized-but-unsupported operations).
///
/// This is the single typed choke point between raw query strings and the
/// [`S3SubResource`] model: [`QueryParameter::parse`] owns the literal name
/// table and every other site matches the typed enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryParameter {
    /// `?uploads` — `CreateMultipartUpload`.
    Uploads,
    /// `?uploadId=<id>`.
    UploadId,
    /// `?partNumber=<n>`.
    PartNumber,
    /// `?list-type=<v>`.
    ListType,
    /// `?delete=` — `DeleteObjects` (batch delete).
    Delete,
    /// `?location`.
    Location,
    /// `?acl`.
    Acl,
    /// `?policy`.
    Policy,
    /// `?lifecycle`.
    Lifecycle,
    /// `?versioning`.
    Versioning,
    /// `?cors`.
    Cors,
    /// `?notification`.
    Notification,
    /// `?tagging`.
    Tagging,
    /// `?encryption`.
    Encryption,
    /// A recognized-but-unsupported sub-resource (for example `restore`,
    /// `select`, `torrent`, `legal-hold`, `retention`, `attributes`,
    /// `replication`, `website`, `versionId`, `versions`, `logging`,
    /// `requestPayment`, `accelerate`, or `object-lock`) — maps to
    /// [`S3SubResource::Other`].
    Other,
}

impl QueryParameter {
    /// Parses a raw query-parameter name into the typed set.
    fn parse(name: &str) -> Option<Self> {
        match name {
            "uploads" => Some(Self::Uploads),
            "uploadId" => Some(Self::UploadId),
            "partNumber" => Some(Self::PartNumber),
            "list-type" => Some(Self::ListType),
            "delete" => Some(Self::Delete),
            "location" => Some(Self::Location),
            "acl" => Some(Self::Acl),
            "policy" => Some(Self::Policy),
            "lifecycle" => Some(Self::Lifecycle),
            "versioning" => Some(Self::Versioning),
            "cors" => Some(Self::Cors),
            "notification" => Some(Self::Notification),
            "tagging" => Some(Self::Tagging),
            "encryption" => Some(Self::Encryption),
            "restore" | "select" | "torrent" | "legal-hold" | "retention" | "attributes"
            | "replication" | "website" | "versionId" | "versions" | "logging"
            | "requestPayment" | "accelerate" | "object-lock" => Some(Self::Other),
            _ => None,
        }
    }
}

/// Parses one query pair into its typed sub-resource, if the name is a
/// recognized sub-resource.
///
/// Plain listing/operation parameters (`prefix`, `delimiter`, `max-keys`,
/// `continuation-token`, `fetch-owner`, …) return `None`.
#[must_use]
pub fn parse_subresource(name: &str, value: &str) -> Option<S3SubResource> {
    match QueryParameter::parse(name)? {
        QueryParameter::Uploads => Some(S3SubResource::Uploads),
        QueryParameter::UploadId => Some(S3SubResource::UploadId(value.to_owned())),
        QueryParameter::PartNumber => match value.parse::<u32>() {
            Ok(number) => Some(S3SubResource::PartNumber(number)),
            Err(_error) => Some(S3SubResource::Other),
        },
        QueryParameter::ListType => Some(if value.parse::<u32>().ok() == Some(2) {
            S3SubResource::ListObjects
        } else {
            S3SubResource::Other
        }),
        QueryParameter::Delete => Some(S3SubResource::DeleteObjects),
        QueryParameter::Location => Some(S3SubResource::Location),
        QueryParameter::Acl => Some(S3SubResource::Acl),
        QueryParameter::Policy => Some(S3SubResource::Policy),
        QueryParameter::Lifecycle => Some(S3SubResource::Lifecycle),
        QueryParameter::Versioning => Some(S3SubResource::Versioning),
        QueryParameter::Cors => Some(S3SubResource::Cors),
        QueryParameter::Notification => Some(S3SubResource::Notification),
        QueryParameter::Tagging => Some(S3SubResource::Tagging),
        QueryParameter::Encryption => Some(S3SubResource::Encryption),
        QueryParameter::Other => Some(S3SubResource::Other),
    }
}

/// Classifies the query parameters of an S3 request into the recognized
/// sub-resources, in query order.
///
/// Plain listing/operation parameters (`prefix`, `delimiter`, `max-keys`,
/// `continuation-token`, `fetch-owner`, …) are not sub-resources and are
/// ignored.
#[must_use]
pub fn classify(query: &QueryMap) -> Vec<S3SubResource> {
    query
        .iter()
        .filter_map(|(name, value)| parse_subresource(name, value))
        .collect()
}

/// Formats a content hash as the S3-style quoted ETag header value.
///
/// # Examples
///
/// ```
/// use shardline_s3_adapter::etag_header;
///
/// assert_eq!(etag_header("ab12"), "\"ab12\"");
/// ```
#[must_use]
pub fn etag_header(content_hash: &str) -> String {
    format!("\"{content_hash}\"")
}

/// Parses an S3 `Range` header against a resource length.
///
/// Reuses [`parse_http_byte_range`]; all parse failures — including
/// unsatisfiable ranges — map to [`S3Error::invalid_range`] (`416
/// InvalidRange`), matching S3 semantics. With no header the full resource
/// range `[0, len-1]` is returned, so callers must handle empty objects
/// (`total == 0`) before calling.
///
/// # Errors
///
/// Returns [`S3Error::invalid_range`] when the header is absent and the
/// resource is empty, or when the header is malformed or unsatisfiable.
pub fn parse_s3_range(header: Option<&str>, total: u64) -> Result<ByteRange, S3Error> {
    let Some(header) = header else {
        let last_byte = total.checked_sub(1).ok_or_else(S3Error::invalid_range)?;
        return ByteRange::new(0, last_byte).map_err(|_error| S3Error::invalid_range());
    };
    parse_http_byte_range(header, total).map_err(|_error| S3Error::invalid_range())
}

/// The entity-tag set of an S3 conditional header.
///
/// [`EntityTagSet::parse`] is the single typed choke point between raw header
/// values and the model; handlers never match strings themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityTagSet {
    /// `*` — matches any existing representation.
    Any,
    /// One or more quoted entity tags (weak `W/` prefixes are stripped per
    /// RFC 9110 weak comparison).
    Tags(Vec<String>),
}

/// A malformed S3 conditional-header (`If-Match` / `If-None-Match`) or
/// `x-amz-copy-source` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidS3HeaderValue;

impl EntityTagSet {
    /// Parses an `If-Match` / `If-None-Match` header value.
    ///
    /// The value is a comma-separated list of quoted entity tags, or the
    /// single `*`. A `W/` weak prefix is stripped (conditional requests use
    /// weak comparison; the stored ETag is the opaque content hash).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidS3HeaderValue`] when the header is empty, an item is
    /// unquoted or empty, or `*` appears alongside concrete tags (which RFC
    /// 9110 forbids).
    pub fn parse(header: &str) -> Result<Self, InvalidS3HeaderValue> {
        match header.trim() {
            "" => Err(InvalidS3HeaderValue),
            "*" => Ok(Self::Any),
            value => {
                let mut tags = Vec::new();
                for item in value.split(',') {
                    let item = item.trim();
                    let item = item.strip_prefix("W/").unwrap_or(item);
                    let Some(inner) = item.strip_prefix('"') else {
                        return Err(InvalidS3HeaderValue);
                    };
                    let Some(inner) = inner.strip_suffix('"') else {
                        return Err(InvalidS3HeaderValue);
                    };
                    if inner.is_empty() {
                        return Err(InvalidS3HeaderValue);
                    }
                    tags.push(inner.to_owned());
                }
                if tags.is_empty() {
                    return Err(InvalidS3HeaderValue);
                }
                Ok(Self::Tags(tags))
            }
        }
    }
}

/// A parsed S3 conditional request header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionalHeader {
    /// `If-Match` — the stored ETag must match (strong).
    IfMatch(EntityTagSet),
    /// `If-None-Match` — the stored ETag must not match (weak).
    IfNoneMatch(EntityTagSet),
}

impl ConditionalHeader {
    /// Evaluates the precondition against the stored ETag (content hash).
    ///
    /// `stored_etag: None` means the object **does not exist**. Per RFC 9110:
    ///
    /// - `If-Match` passes when the object exists and any listed tag equals the
    ///   stored ETag (`*` passes when the object exists at all);
    /// - `If-None-Match` passes when no listed tag equals the stored ETag, or
    ///   when the object does not exist (`*` passes only when the object does
    ///   not exist).
    #[must_use]
    pub fn satisfied(&self, stored_etag: Option<&str>) -> bool {
        match self {
            Self::IfMatch(tags) => match tags {
                EntityTagSet::Any => stored_etag.is_some(),
                EntityTagSet::Tags(tags) => {
                    stored_etag.is_some_and(|etag| tags.iter().any(|tag| tag == etag))
                }
            },
            Self::IfNoneMatch(tags) => match tags {
                EntityTagSet::Any => stored_etag.is_none(),
                EntityTagSet::Tags(tags) => {
                    stored_etag.is_none_or(|etag| !tags.iter().any(|tag| tag == etag))
                }
            },
        }
    }
}

/// Reads the S3 conditional headers (`If-Match` / `If-None-Match`) from a
/// request's header map.
///
/// Per RFC 9110 precedence, `If-Match` wins over `If-None-Match` when both are
/// present, so only the stronger condition is returned.
#[must_use]
pub fn read_conditional_headers(headers: &HeaderMap) -> Option<ConditionalHeader> {
    if let Some(value) = headers.get(IF_MATCH).and_then(header_value_str) {
        return EntityTagSet::parse(value)
            .ok()
            .map(ConditionalHeader::IfMatch);
    }
    headers
        .get(IF_NONE_MATCH)
        .and_then(header_value_str)
        .and_then(|value| EntityTagSet::parse(value).ok())
        .map(ConditionalHeader::IfNoneMatch)
}

/// Borrows a header value as a string when it is valid ASCII (HTTP headers are
/// latin-1; a non-ASCII value is treated as absent).
fn header_value_str(value: &axum::http::HeaderValue) -> Option<&str> {
    value.to_str().ok()
}

/// A parsed `x-amz-copy-source` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySource {
    /// The source bucket (`{owner}.{name}`).
    pub bucket: String,
    /// The source object key.
    pub key: String,
}

/// Parses an S3 `x-amz-copy-source` header value (`/{bucket}/{key}`, with the
/// leading slash optional) into its typed bucket + key parts.
///
/// # Errors
///
/// Returns [`InvalidS3HeaderValue`] when the source has no `/` separator or
/// either part is empty.
pub fn parse_copy_source(source: &str) -> Result<CopySource, InvalidS3HeaderValue> {
    let trimmed = source.trim_start_matches('/');
    let Some((bucket, key)) = trimmed.split_once('/') else {
        return Err(InvalidS3HeaderValue);
    };
    if bucket.is_empty() || key.is_empty() {
        return Err(InvalidS3HeaderValue);
    }
    Ok(CopySource {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    })
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

    use super::*;

    fn query(entries: &[(&str, &str)]) -> QueryMap {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn classify_recognizes_multipart_uploads() {
        assert_eq!(
            classify(&query(&[("uploads", "")])),
            vec![S3SubResource::Uploads]
        );
    }

    #[test]
    fn classify_recognizes_upload_id() {
        assert_eq!(
            classify(&query(&[("uploadId", "session-42")])),
            vec![S3SubResource::UploadId("session-42".to_owned())]
        );
    }

    #[test]
    fn classify_recognizes_part_number() {
        assert_eq!(
            classify(&query(&[("partNumber", "3")])),
            vec![S3SubResource::PartNumber(3)]
        );
    }

    #[test]
    fn classify_maps_malformed_part_number_to_other() {
        assert_eq!(
            classify(&query(&[("partNumber", "not-a-number")])),
            vec![S3SubResource::Other]
        );
    }

    #[test]
    fn classify_recognizes_list_type_two() {
        assert_eq!(
            classify(&query(&[("list-type", "2")])),
            vec![S3SubResource::ListObjects]
        );
        // `list-type=1` (ListObjectsV1) is out of scope.
        assert_eq!(
            classify(&query(&[("list-type", "1")])),
            vec![S3SubResource::Other]
        );
    }

    #[test]
    fn classify_recognizes_location() {
        assert_eq!(
            classify(&query(&[("location", "")])),
            vec![S3SubResource::Location]
        );
    }

    #[test]
    fn classify_recognizes_out_of_scope_sub_resources() {
        let cases = [
            ("acl", S3SubResource::Acl),
            ("policy", S3SubResource::Policy),
            ("lifecycle", S3SubResource::Lifecycle),
            ("versioning", S3SubResource::Versioning),
            ("cors", S3SubResource::Cors),
            ("notification", S3SubResource::Notification),
            ("tagging", S3SubResource::Tagging),
            ("encryption", S3SubResource::Encryption),
        ];
        for (name, expected) in cases {
            assert_eq!(
                classify(&query(&[(name, "")])),
                vec![expected],
                "{name} must classify as its sub-resource"
            );
        }
    }

    #[test]
    fn classify_recognizes_additional_out_of_scope_sub_resources_as_other() {
        for name in [
            "restore",
            "select",
            "torrent",
            "legal-hold",
            "retention",
            "attributes",
            "replication",
            "website",
            "versionId",
            "versions",
            "logging",
            "requestPayment",
            "accelerate",
            "object-lock",
        ] {
            assert_eq!(
                classify(&query(&[(name, "")])),
                vec![S3SubResource::Other],
                "{name} must classify as Other"
            );
        }
    }

    #[test]
    fn classify_ignores_plain_listing_parameters() {
        let result = classify(&query(&[
            ("prefix", "dir/"),
            ("delimiter", "/"),
            ("max-keys", "1000"),
            ("continuation-token", "abc"),
        ]));
        assert!(result.is_empty());
    }

    #[test]
    fn classify_preserves_query_order_for_multiple_sub_resources() {
        let result = classify(&query(&[
            ("partNumber", "1"),
            ("uploadId", "session-1"),
            ("uploads", ""),
        ]));
        assert_eq!(
            result,
            vec![
                S3SubResource::PartNumber(1),
                S3SubResource::UploadId("session-1".to_owned()),
                S3SubResource::Uploads,
            ]
        );
    }

    #[test]
    fn etag_header_quotes_the_content_hash() {
        assert_eq!(etag_header("ab12"), "\"ab12\"");
        assert_eq!(
            etag_header("a".repeat(64).as_str()),
            format!("\"{}\"", "a".repeat(64))
        );
    }

    #[test]
    fn parse_s3_range_absent_header_returns_full_range() {
        assert_eq!(
            parse_s3_range(None, 100).unwrap(),
            ByteRange::new(0, 99).unwrap()
        );
    }

    #[test]
    fn parse_s3_range_closed_range() {
        assert_eq!(
            parse_s3_range(Some("bytes=0-4"), 100).unwrap(),
            ByteRange::new(0, 4).unwrap()
        );
    }

    #[test]
    fn parse_s3_range_open_ended_is_clamped() {
        assert_eq!(
            parse_s3_range(Some("bytes=95-"), 100).unwrap(),
            ByteRange::new(95, 99).unwrap()
        );
    }

    #[test]
    fn parse_s3_range_suffix_range() {
        assert_eq!(
            parse_s3_range(Some("bytes=-3"), 100).unwrap(),
            ByteRange::new(97, 99).unwrap()
        );
    }

    #[test]
    fn parse_s3_range_unsatisfiable_maps_to_invalid_range() {
        let error = parse_s3_range(Some("bytes=100-"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        assert_eq!(error.status, 416);
        let error = parse_s3_range(Some("bytes=5-2"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
    }

    #[test]
    fn parse_s3_range_malformed_maps_to_invalid_range() {
        for header in ["5-10", "bytes=", "bytes=abc", "bytes=0-4,8-9"] {
            let error = parse_s3_range(Some(header), 100).unwrap_err();
            assert_eq!(error.code, "InvalidRange", "header {header:?}");
        }
    }

    #[test]
    fn parse_s3_range_empty_resource_is_invalid() {
        let error = parse_s3_range(None, 0).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        let error = parse_s3_range(Some("bytes=-5"), 0).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
    }

    #[test]
    fn parse_s3_range_edge_cases() {
        // Open-ended from zero.
        assert_eq!(
            parse_s3_range(Some("bytes=0-"), 100).unwrap(),
            ByteRange::new(0, 99).unwrap()
        );
        // A zero-length suffix is invalid.
        let error = parse_s3_range(Some("bytes=-0"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        assert_eq!(error.status, 416);
        // Inverted range.
        let error = parse_s3_range(Some("bytes=5-2"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        // A huge end is clamped to the resource length.
        assert_eq!(
            parse_s3_range(Some("bytes=0-18446744073709551615"), 100).unwrap(),
            ByteRange::new(0, 99).unwrap()
        );
        // Multi-range is rejected.
        let error = parse_s3_range(Some("bytes=0-1,3-4"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        // The byte-range unit must be lowercase `bytes=`.
        for header in ["Bytes=0-1", "BYTES=0-1", "bytes =0-1"] {
            let error = parse_s3_range(Some(header), 100).unwrap_err();
            assert_eq!(error.code, "InvalidRange", "header {header:?}");
        }
        // Single-byte open-ended range.
        assert_eq!(
            parse_s3_range(Some("bytes=99-"), 100).unwrap(),
            ByteRange::new(99, 99).unwrap()
        );
        // Start beyond the resource.
        let error = parse_s3_range(Some("bytes=100-"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        // Numeric overflow in the end is invalid syntax.
        let error = parse_s3_range(Some("bytes=0-99999999999999999999999"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
        // Whitespace padding is not accepted.
        let error = parse_s3_range(Some(" bytes=0-1"), 100).unwrap_err();
        assert_eq!(error.code, "InvalidRange");
    }

    // ── conditional headers ────────────────────────────────────────────────

    #[test]
    fn entity_tag_set_parses_single_and_multi_tag_lists() {
        assert_eq!(
            EntityTagSet::parse("\"abc123\"").unwrap(),
            EntityTagSet::Tags(vec!["abc123".to_owned()])
        );
        assert_eq!(
            EntityTagSet::parse("\"a\", \"b\",\"c\"").unwrap(),
            EntityTagSet::Tags(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
    }

    #[test]
    fn entity_tag_set_strips_weak_prefix_and_star() {
        assert_eq!(
            EntityTagSet::parse("W/\"abc123\"").unwrap(),
            EntityTagSet::Tags(vec!["abc123".to_owned()])
        );
        assert_eq!(EntityTagSet::parse(" * ").unwrap(), EntityTagSet::Any);
    }

    #[test]
    fn entity_tag_set_rejects_malformed_values() {
        for value in ["", "abc", "\"\"", "abc,", "*,\"a\"", "\"a\",*"] {
            assert!(EntityTagSet::parse(value).is_err(), "value {value:?}");
        }
    }

    #[test]
    fn conditional_if_match_satisfied_only_when_stored_etag_matches() {
        let match_tag = ConditionalHeader::IfMatch(EntityTagSet::Tags(vec!["hash-a".to_owned()]));
        assert!(match_tag.satisfied(Some("hash-a")));
        assert!(!match_tag.satisfied(Some("hash-b")));
        assert!(!match_tag.satisfied(None), "missing object fails If-Match");

        let match_any = ConditionalHeader::IfMatch(EntityTagSet::Any);
        assert!(match_any.satisfied(Some("hash-a")));
        assert!(
            !match_any.satisfied(None),
            "* fails when the object is missing"
        );
    }

    #[test]
    fn conditional_if_none_match_satisfied_on_missing_or_non_matching() {
        let none_match_tag =
            ConditionalHeader::IfNoneMatch(EntityTagSet::Tags(vec!["hash-a".to_owned()]));
        assert!(none_match_tag.satisfied(Some("hash-b")));
        assert!(!none_match_tag.satisfied(Some("hash-a")));
        assert!(
            none_match_tag.satisfied(None),
            "missing object passes If-None-Match"
        );

        let none_match_any = ConditionalHeader::IfNoneMatch(EntityTagSet::Any);
        assert!(none_match_any.satisfied(None));
        assert!(
            !none_match_any.satisfied(Some("hash-a")),
            "* fails when the object exists"
        );
    }

    #[test]
    fn read_conditional_headers_prefers_if_match_over_if_none_match() {
        let mut headers = HeaderMap::new();
        headers.insert(IF_MATCH, "\"abc\"".parse().unwrap());
        headers.insert(IF_NONE_MATCH, "*".parse().unwrap());
        assert_eq!(
            read_conditional_headers(&headers),
            Some(ConditionalHeader::IfMatch(EntityTagSet::Tags(vec![
                "abc".to_owned()
            ])))
        );

        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, "*".parse().unwrap());
        assert_eq!(
            read_conditional_headers(&headers),
            Some(ConditionalHeader::IfNoneMatch(EntityTagSet::Any))
        );

        let headers = HeaderMap::new();
        assert_eq!(read_conditional_headers(&headers), None);
    }

    // ── x-amz-copy-source ──────────────────────────────────────────────────

    #[test]
    fn parse_copy_source_splits_bucket_and_key() {
        assert_eq!(
            parse_copy_source("/acme.models/data/model.pt").unwrap(),
            CopySource {
                bucket: "acme.models".to_owned(),
                key: "data/model.pt".to_owned(),
            }
        );
        assert_eq!(
            parse_copy_source("acme.models/file.txt").unwrap(),
            CopySource {
                bucket: "acme.models".to_owned(),
                key: "file.txt".to_owned(),
            }
        );
    }

    #[test]
    fn parse_copy_source_rejects_missing_or_empty_parts() {
        for value in ["", "/", "acme.models", "/acme.models/", "acme.models/"] {
            assert!(parse_copy_source(value).is_err(), "value {value:?}");
        }
    }
}
