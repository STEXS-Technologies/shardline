use axum::http::{
    HeaderMap, HeaderValue,
    header::InvalidHeaderValue,
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED},
};

use crate::protocol_support::etag_header;

/// Escapes a string for XML text content: the five predefined XML entities plus
/// a strip of XML-1.0-invalid control characters (so a hostile key, bucket, or
/// message can never produce a malformed envelope).
fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\u{0}'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' => {}
            _ => out.push(ch),
        }
    }
    out
}

/// The S3 error envelope body (`<Error><Code>…</Code>…`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ErrorBody {
    /// The S3 error code, for example `NoSuchKey`.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// The object key the error refers to, when applicable.
    pub key: Option<String>,
    /// The request identifier for correlation, when available.
    pub request_id: Option<String>,
}

impl S3ErrorBody {
    /// Serializes the error body to the S3 XML error envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        let key = self
            .key
            .as_ref()
            .map(|value| format!("  <Key>{}</Key>\n", xml_escape(value)))
            .unwrap_or_default();
        let request_id = self
            .request_id
            .as_ref()
            .map(|value| format!("  <RequestId>{}</RequestId>\n", xml_escape(value)))
            .unwrap_or_default();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n{key}{request_id}</Error>\n",
            xml_escape(&self.code),
            xml_escape(&self.message),
        )
    }
}

/// One listed object entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contents {
    /// The client-facing S3 object key.
    pub key: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// The BLAKE3 root content hash; serialized quoted as the S3 ETag.
    pub etag: String,
    /// `LastModified` value in ISO-8601 format (for example
    /// `2026-08-13T09:51:00Z`).
    pub last_modified_iso8601: String,
}

impl Contents {
    fn to_xml(&self) -> String {
        format!(
            "  <Contents>\n    <Key>{}</Key>\n    <Size>{}</Size>\n    <ETag>{}</ETag>\n    <LastModified>{}</LastModified>\n  </Contents>\n",
            xml_escape(&self.key),
            self.size_bytes,
            etag_header(&self.etag),
            xml_escape(&self.last_modified_iso8601),
        )
    }
}

/// The `ListObjectsV2` response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketResult {
    /// The listed object entries, in raw-key order.
    pub contents: Vec<Contents>,
    /// Common-prefix rollups (the `delimiter`/`prefix` grouping).
    pub common_prefixes: Vec<String>,
    /// Whether more keys exist beyond the returned page.
    pub is_truncated: bool,
    /// Opaque keyset cursor for the next page.
    pub next_continuation_token: Option<String>,
}

impl ListBucketResult {
    /// Serializes the result to the S3 `ListBucketResult` XML envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        let contents = self
            .contents
            .iter()
            .map(Contents::to_xml)
            .collect::<String>();
        let common_prefixes = self
            .common_prefixes
            .iter()
            .map(|prefix| {
                format!(
                    "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
                    xml_escape(prefix)
                )
            })
            .collect::<String>();
        let continuation = self
            .next_continuation_token
            .as_ref()
            .map(|token| {
                format!(
                    "  <NextContinuationToken>{}</NextContinuationToken>\n",
                    xml_escape(token)
                )
            })
            .unwrap_or_default();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n{contents}{common_prefixes}  <IsTruncated>{}</IsTruncated>\n{continuation}</ListBucketResult>\n",
            self.is_truncated,
        )
    }
}

/// The `CompleteMultipartUpload` response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteMultipartUploadResult {
    /// The bucket name (`{owner}.{name}`).
    pub bucket: String,
    /// The completed object key.
    pub key: String,
    /// The BLAKE3 root content hash; serialized quoted as the S3 ETag.
    pub etag: String,
}

impl CompleteMultipartUploadResult {
    /// Serializes the result to the S3 `CompleteMultipartUploadResult` XML
    /// envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>{}</ETag>\n</CompleteMultipartUploadResult>\n",
            xml_escape(&self.bucket),
            xml_escape(&self.key),
            etag_header(&self.etag),
        )
    }
}

/// Response headers for a successful `PutObject`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectResponseHeaders {
    /// The BLAKE3 root content hash (served quoted as the ETag).
    pub etag: String,
    /// The response body length in bytes (0 for a bodyless PUT response).
    pub content_length: u64,
    /// The stored content type.
    pub content_type: String,
    /// `Last-Modified` in ISO-8601 format.
    pub last_modified_iso8601: String,
}

impl PutObjectResponseHeaders {
    /// Writes the header set into a fresh [`HeaderMap`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidHeaderValue`] when a field cannot be represented as an
    /// HTTP header value.
    pub fn to_header_map(&self) -> Result<HeaderMap, InvalidHeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_str(&etag_header(&self.etag))?);
        headers.insert(CONTENT_LENGTH, HeaderValue::from(self.content_length));
        headers.insert(CONTENT_TYPE, HeaderValue::from_str(&self.content_type)?);
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&self.last_modified_iso8601)?,
        );
        Ok(headers)
    }
}

/// Response headers for `HeadObject`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObjectHeaders {
    /// The BLAKE3 root content hash (served quoted as the ETag).
    pub etag: String,
    /// The object length in bytes.
    pub content_length: u64,
    /// The stored content type.
    pub content_type: String,
    /// `Last-Modified` in ISO-8601 format.
    pub last_modified_iso8601: String,
}

impl HeadObjectHeaders {
    /// Writes the header set into a fresh [`HeaderMap`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidHeaderValue`] when a field cannot be represented as an
    /// HTTP header value.
    pub fn to_header_map(&self) -> Result<HeaderMap, InvalidHeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert(ETAG, HeaderValue::from_str(&etag_header(&self.etag))?);
        headers.insert(CONTENT_LENGTH, HeaderValue::from(self.content_length));
        headers.insert(CONTENT_TYPE, HeaderValue::from_str(&self.content_type)?);
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&self.last_modified_iso8601)?,
        );
        Ok(headers)
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

    use super::*;

    #[test]
    fn error_body_xml_golden_minimal() {
        let body = S3ErrorBody {
            code: "NoSuchBucket".to_owned(),
            message: "The specified bucket does not exist.".to_owned(),
            key: None,
            request_id: None,
        };
        assert_eq!(
            body.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error>\n\
             \x20 <Code>NoSuchBucket</Code>\n\
             \x20 <Message>The specified bucket does not exist.</Message>\n\
             </Error>\n"
        );
    }

    #[test]
    fn error_body_xml_golden_with_optional_fields() {
        let body = S3ErrorBody {
            code: "NoSuchKey".to_owned(),
            message: "The specified key does not exist.".to_owned(),
            key: Some("data/model.pt".to_owned()),
            request_id: Some("req-123".to_owned()),
        };
        assert_eq!(
            body.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error>\n\
             \x20 <Code>NoSuchKey</Code>\n\
             \x20 <Message>The specified key does not exist.</Message>\n\
             \x20 <Key>data/model.pt</Key>\n\
             \x20 <RequestId>req-123</RequestId>\n\
             </Error>\n"
        );
    }

    #[test]
    fn error_body_xml_escapes_special_characters() {
        let body = S3ErrorBody {
            code: "NoSuchKey".to_owned(),
            message: "bad <>&\"' key".to_owned(),
            key: Some("a<b>&\"'c.txt".to_owned()),
            request_id: None,
        };
        let xml = body.to_xml();
        assert!(xml.contains("<Message>bad &lt;&gt;&amp;&quot;&apos; key</Message>"));
        assert!(xml.contains("<Key>a&lt;b&gt;&amp;&quot;&apos;c.txt</Key>"));
        assert!(!xml.contains("a<b>"));
    }

    #[test]
    fn error_body_xml_strips_xml_invalid_control_characters() {
        let body = S3ErrorBody {
            code: "InternalError".to_owned(),
            message: "boom\u{7}".to_owned(),
            key: None,
            request_id: None,
        };
        assert!(!body.to_xml().contains('\u{7}'));
    }

    #[test]
    fn list_bucket_result_xml_golden() {
        let result = ListBucketResult {
            contents: vec![Contents {
                key: "a.txt".to_owned(),
                size_bytes: 123,
                etag: "ab12".to_owned(),
                last_modified_iso8601: "2026-08-13T09:51:00Z".to_owned(),
            }],
            common_prefixes: vec!["dir/".to_owned()],
            is_truncated: true,
            next_continuation_token: Some("token-42".to_owned()),
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Contents>\n\
             \x20\x20\x20\x20<Key>a.txt</Key>\n\
             \x20\x20\x20\x20<Size>123</Size>\n\
             \x20\x20\x20\x20<ETag>\"ab12\"</ETag>\n\
             \x20\x20\x20\x20<LastModified>2026-08-13T09:51:00Z</LastModified>\n\
             \x20 </Contents>\n\
             \x20 <CommonPrefixes>\n\
             \x20\x20\x20\x20<Prefix>dir/</Prefix>\n\
             \x20 </CommonPrefixes>\n\
             \x20 <IsTruncated>true</IsTruncated>\n\
             \x20 <NextContinuationToken>token-42</NextContinuationToken>\n\
             </ListBucketResult>\n"
        );
    }

    #[test]
    fn list_bucket_result_xml_empty() {
        let result = ListBucketResult {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_continuation_token: None,
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <IsTruncated>false</IsTruncated>\n\
             </ListBucketResult>\n"
        );
    }

    #[test]
    fn complete_multipart_upload_result_xml_golden() {
        let result = CompleteMultipartUploadResult {
            bucket: "acme.models".to_owned(),
            key: "data/model.pt".to_owned(),
            etag: "cd34".to_owned(),
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Bucket>acme.models</Bucket>\n\
             \x20 <Key>data/model.pt</Key>\n\
             \x20 <ETag>\"cd34\"</ETag>\n\
             </CompleteMultipartUploadResult>\n"
        );
    }

    #[test]
    fn put_object_headers_roundtrip() {
        let headers = PutObjectResponseHeaders {
            etag: "ab12".to_owned(),
            content_length: 0,
            content_type: "application/octet-stream".to_owned(),
            last_modified_iso8601: "2026-08-13T09:51:00Z".to_owned(),
        }
        .to_header_map()
        .unwrap();
        assert_eq!(
            headers.get(ETAG).unwrap().to_str().unwrap(),
            "\"ab12\"",
            "etag must be served quoted"
        );
        assert_eq!(headers.get(CONTENT_LENGTH).unwrap().to_str().unwrap(), "0");
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "application/octet-stream"
        );
        assert_eq!(
            headers.get(LAST_MODIFIED).unwrap().to_str().unwrap(),
            "2026-08-13T09:51:00Z"
        );
    }

    #[test]
    fn head_object_headers_roundtrip() {
        let headers = HeadObjectHeaders {
            etag: "ef56".to_owned(),
            content_length: 4096,
            content_type: "text/plain".to_owned(),
            last_modified_iso8601: "2026-08-13T09:51:00Z".to_owned(),
        }
        .to_header_map()
        .unwrap();
        assert_eq!(headers.get(ETAG).unwrap().to_str().unwrap(), "\"ef56\"");
        assert_eq!(
            headers.get(CONTENT_LENGTH).unwrap().to_str().unwrap(),
            "4096"
        );
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "text/plain"
        );
        assert_eq!(
            headers.get(LAST_MODIFIED).unwrap().to_str().unwrap(),
            "2026-08-13T09:51:00Z"
        );
    }
}
