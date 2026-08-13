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

/// Classifies the query parameters of an S3 request into the recognized
/// sub-resources, in query order.
///
/// Plain listing/operation parameters (`prefix`, `delimiter`, `max-keys`,
/// `continuation-token`, `fetch-owner`, …) are not sub-resources and are
/// ignored.
#[must_use]
pub fn classify(query: &QueryMap) -> Vec<S3SubResource> {
    let mut resources = Vec::new();
    for (name, value) in query {
        match name.as_str() {
            "uploads" => resources.push(S3SubResource::Uploads),
            "uploadId" => resources.push(S3SubResource::UploadId(value.clone())),
            "partNumber" => match value.parse::<u32>() {
                Ok(number) => resources.push(S3SubResource::PartNumber(number)),
                Err(_error) => resources.push(S3SubResource::Other),
            },
            "list-type" if value == "2" => resources.push(S3SubResource::ListObjects),
            "list-type" => resources.push(S3SubResource::Other),
            "location" => resources.push(S3SubResource::Location),
            "acl" => resources.push(S3SubResource::Acl),
            "policy" => resources.push(S3SubResource::Policy),
            "lifecycle" => resources.push(S3SubResource::Lifecycle),
            "versioning" => resources.push(S3SubResource::Versioning),
            "cors" => resources.push(S3SubResource::Cors),
            "notification" => resources.push(S3SubResource::Notification),
            "tagging" => resources.push(S3SubResource::Tagging),
            "encryption" => resources.push(S3SubResource::Encryption),
            "restore" | "select" | "torrent" | "legal-hold" | "retention" | "attributes"
            | "replication" | "website" | "versionId" | "versions" | "logging"
            | "requestPayment" | "accelerate" | "object-lock" => {
                resources.push(S3SubResource::Other);
            }
            _ => {}
        }
    }
    resources
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
}
