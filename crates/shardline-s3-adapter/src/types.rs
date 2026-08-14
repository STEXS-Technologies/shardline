use std::collections::BTreeSet;

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

/// The `ListObjects` (v1) response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketResultV1 {
    /// The listed object entries, in raw-key order.
    pub contents: Vec<Contents>,
    /// Common-prefix rollups (the `delimiter`/`prefix` grouping).
    pub common_prefixes: Vec<String>,
    /// The bucket name.
    pub name: String,
    /// The requested prefix filter.
    pub prefix: String,
    /// The requested resume marker (empty when none was sent).
    pub marker: String,
    /// The page row budget.
    pub max_keys: usize,
    /// The grouping delimiter (when one was requested).
    pub delimiter: Option<String>,
    /// Whether more keys exist beyond the returned page.
    pub is_truncated: bool,
    /// The raw key the next page resumes after (when truncated).
    pub next_marker: Option<String>,
}

impl ListBucketResultV1 {
    /// Serializes the result to the S3 `ListBucketResult` (v1) XML envelope.
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
        let delimiter = self
            .delimiter
            .as_ref()
            .map(|delimiter| format!("  <Delimiter>{}</Delimiter>\n", xml_escape(delimiter)))
            .unwrap_or_default();
        let next_marker = self
            .next_marker
            .as_ref()
            .map(|marker| format!("  <NextMarker>{}</NextMarker>\n", xml_escape(marker)))
            .unwrap_or_default();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Name>{}</Name>\n  <Prefix>{}</Prefix>\n  <Marker>{}</Marker>\n  <MaxKeys>{}</MaxKeys>\n{delimiter}{contents}{common_prefixes}  <IsTruncated>{}</IsTruncated>\n{next_marker}</ListBucketResult>\n",
            xml_escape(&self.name),
            xml_escape(&self.prefix),
            xml_escape(&self.marker),
            self.max_keys,
            self.is_truncated,
        )
    }
}

/// The `ListAllMyBucketsResult` (service-level `GET /`) response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketsResult {
    /// The bucket names owned by the caller (`{owner}.{name}`).
    pub buckets: Vec<String>,
}

impl ListBucketsResult {
    /// Serializes the result to the S3 `ListAllMyBucketsResult` XML envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        let mut xml = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Buckets>\n",
        );
        for bucket in &self.buckets {
            xml.push_str("    <Bucket><Name>");
            xml.push_str(&xml_escape(bucket));
            xml.push_str("</Name></Bucket>\n");
        }
        xml.push_str("  </Buckets>\n</ListAllMyBucketsResult>\n");
        xml
    }
}

/// The `CopyObject` response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyObjectResult {
    /// The BLAKE3 root content hash of the copied object; serialized quoted as
    /// the S3 ETag (identical content → identical ETag).
    pub etag: String,
    /// `LastModified` in ISO-8601 format.
    pub last_modified_iso8601: String,
}

impl CopyObjectResult {
    /// Serializes the result to the S3 `CopyObjectResult` XML envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <ETag>{}</ETag>\n  <LastModified>{}</LastModified>\n</CopyObjectResult>\n",
            etag_header(&self.etag),
            xml_escape(&self.last_modified_iso8601),
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

/// The `InitiateMultipartUpload` response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateMultipartUploadResult {
    /// The bucket name (`{owner}.{name}`).
    pub bucket: String,
    /// The completed object key.
    pub key: String,
    /// The opaque upload id for the new multipart upload session.
    pub upload_id: String,
}

impl InitiateMultipartUploadResult {
    /// Serializes the result to the S3 `InitiateMultipartUploadResult` XML
    /// envelope.
    #[must_use]
    pub fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>\n",
            xml_escape(&self.bucket),
            xml_escape(&self.key),
            xml_escape(&self.upload_id),
        )
    }
}

/// The element names the `CompleteMultipartUpload` body scanner recognizes.
///
/// [`CompleteXmlElement::parse`] is the single typed choke point between raw
/// XML element names and the model; the scanner never matches strings itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompleteXmlElement {
    /// The `CompleteMultipartUpload` root element.
    CompleteMultipartUpload,
    /// A `Part` element.
    Part,
    /// The `PartNumber` element (the only content the scanner reads).
    PartNumber,
    /// An `ETag` element (opaque; ignored).
    ETag,
    /// The `Delete` root element of a `DeleteObjects` request.
    Delete,
    /// The `Object` element of a `DeleteObjects` request.
    Object,
    /// The `Key` element of a `DeleteObjects` request.
    Key,
    /// The `Quiet` element of a `DeleteObjects` request.
    Quiet,
    /// Any other element (ignored).
    Other,
}

impl CompleteXmlElement {
    /// Parses a raw XML element name into the typed set.
    fn parse(name: &str) -> Self {
        match name {
            "CompleteMultipartUpload" => Self::CompleteMultipartUpload,
            "Part" => Self::Part,
            "PartNumber" => Self::PartNumber,
            "ETag" => Self::ETag,
            "Delete" => Self::Delete,
            "Object" => Self::Object,
            "Key" => Self::Key,
            "Quiet" => Self::Quiet,
            _ => Self::Other,
        }
    }
}

/// One event from the bounded [`CompleteXmlScanner`].
enum XmlEvent<'value> {
    /// An opening element (or a self-closing / processing-instruction tag).
    Open(CompleteXmlElement),
    /// A closing element.
    Close(CompleteXmlElement),
    /// Character data between tags.
    Text(&'value str),
    /// The end of the input.
    End,
}

/// A minimal, bounded XML tokenizer for the `CompleteMultipartUpload` body.
///
/// It scans for `<`/`>` tags, classifies element names through
/// [`CompleteXmlElement::parse`], and yields text between tags. Processing
/// instructions (`<?…?>`) and comments (`<!…>`) are skipped as non-matching
/// elements; self-closing tags yield an open event only. Malformed input is
/// tolerated (any tag that never closes terminates the scan).
struct CompleteXmlScanner<'value> {
    body: &'value str,
    offset: usize,
}

impl<'value> CompleteXmlScanner<'value> {
    const fn new(body: &'value str) -> Self {
        Self { body, offset: 0 }
    }

    /// Yields the next XML event.
    ///
    /// # Errors
    ///
    /// Returns [`crate::S3Error::invalid_part`] when the input contains an
    /// unterminated tag (no closing `>`).
    fn next_event(&mut self) -> Result<XmlEvent<'value>, crate::S3Error> {
        let rest = self
            .body
            .get(self.offset..)
            .ok_or_else(crate::S3Error::invalid_part)?;
        let Some(next_tag) = rest.find('<') else {
            let trailing = rest;
            self.offset = self.body.len();
            return if trailing.is_empty() {
                Ok(XmlEvent::End)
            } else {
                Ok(XmlEvent::Text(trailing))
            };
        };
        if next_tag > 0 {
            let text = rest
                .get(..next_tag)
                .ok_or_else(crate::S3Error::invalid_part)?;
            self.offset = self.offset.saturating_add(next_tag);
            return Ok(XmlEvent::Text(text));
        }
        // At '<': read the tag through its closing '>'.
        let tagged = self
            .body
            .get(self.offset..)
            .ok_or_else(crate::S3Error::invalid_part)?;
        let Some(close) = tagged.find('>') else {
            return Err(crate::S3Error::invalid_part());
        };
        let tag = tagged
            .get(..=close)
            .ok_or_else(crate::S3Error::invalid_part)?;
        self.offset = self.offset.saturating_add(tag.len());
        let is_closing = tag.as_bytes().get(1) == Some(&b'/');
        let name_start = if is_closing { 2 } else { 1 };
        let tag_after_name = tag.get(name_start..).unwrap_or("");
        let name_end = tag_after_name
            .find(|ch: char| ch.is_whitespace() || ch == '>' || ch == '/')
            .unwrap_or(tag_after_name.len());
        let name = tag_after_name.get(..name_end).unwrap_or("");
        if is_closing {
            Ok(XmlEvent::Close(CompleteXmlElement::parse(name)))
        } else {
            Ok(XmlEvent::Open(CompleteXmlElement::parse(name)))
        }
    }
}

/// The parsed `CompleteMultipartUpload` request body.
///
/// Only the `PartNumber` elements are read (ETags are opaque and ignored);
/// duplicate part numbers collapse, and the numbers are kept in sorted order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompleteParts {
    part_numbers: BTreeSet<u32>,
}

impl CompleteParts {
    /// Returns the part numbers in sorted order.
    #[must_use]
    pub const fn part_numbers(&self) -> &BTreeSet<u32> {
        &self.part_numbers
    }

    /// The number of distinct parts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.part_numbers.len()
    }

    /// Whether no parts were listed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.part_numbers.is_empty()
    }

    /// The largest part number, if any.
    #[must_use]
    pub fn max_part(&self) -> Option<u32> {
        self.part_numbers.last().copied()
    }
}

/// Parses the part numbers from a `CompleteMultipartUpload` request body.
///
/// The body is the S3
/// `<CompleteMultipartUpload><Part><PartNumber>N</PartNumber><ETag>…</ETag></Part>…</CompleteMultipartUpload>`
/// envelope. Parsing uses a bounded XML tokenizer; only `PartNumber` element
/// text is read (ETags are ignored) and every part number is validated against
/// `1..=MAX_S3_PART_NUMBER`. Duplicate numbers collapse into a set.
///
/// # Errors
///
/// Returns [`crate::S3Error::invalid_part`] when no valid part numbers are
/// present, a part number is not a valid `u32` within `1..=10000`, or the body
/// contains an unterminated tag.
pub fn parse_complete_multipart_parts(body: &str) -> Result<CompleteParts, crate::S3Error> {
    let mut scanner = CompleteXmlScanner::new(body);
    let mut parts = BTreeSet::new();
    let mut pending_part_number: Option<String> = None;
    loop {
        match scanner.next_event()? {
            XmlEvent::Open(element) => {
                if element == CompleteXmlElement::PartNumber {
                    pending_part_number = Some(String::new());
                }
            }
            XmlEvent::Text(text) => {
                if let Some(buffer) = pending_part_number.as_mut() {
                    buffer.push_str(text);
                }
            }
            XmlEvent::Close(element) => {
                if element == CompleteXmlElement::PartNumber
                    && let Some(raw_number) = pending_part_number.take()
                {
                    let number = raw_number
                        .trim()
                        .parse::<u32>()
                        .map_err(|_error| crate::S3Error::invalid_part())?;
                    if number == 0 || number > crate::multipart::MAX_S3_PART_NUMBER {
                        return Err(crate::S3Error::invalid_part());
                    }
                    parts.insert(number);
                }
            }
            XmlEvent::End => break,
        }
    }
    if parts.is_empty() {
        return Err(crate::S3Error::invalid_part());
    }
    Ok(CompleteParts {
        part_numbers: parts,
    })
}

/// The maximum number of keys in a single `DeleteObjects` request.
///
/// S3's published limit is 1000 keys per batch delete; exceeding it is
/// `MalformedXML` (`400`). The same constant bounds the `<DeleteResult>`
/// response, keeping both the backend work (two ops per key) and the response
/// size linear in the protocol cap.
pub const MAX_S3_DELETE_KEYS: usize = 1000;

/// Parses the `<Key>` values from a `DeleteObjects` request body.
///
/// The body is the S3
/// `<Delete><Object><Key>k</Key></Object>…</Delete>` envelope. Only `Key`
/// element text is read (the `Quiet` flag is ignored); every key is returned
/// in document order (duplicates preserved). Empty keys are skipped.
///
/// # Errors
///
/// Returns [`crate::S3Error::invalid_part`] when the body contains an
/// unterminated tag.
pub fn parse_delete_object_keys(body: &str) -> Result<Vec<String>, crate::S3Error> {
    let mut scanner = CompleteXmlScanner::new(body);
    let mut keys = Vec::new();
    let mut pending_key: Option<String> = None;
    loop {
        match scanner.next_event()? {
            XmlEvent::Open(element) => {
                if element == CompleteXmlElement::Key {
                    pending_key = Some(String::new());
                }
            }
            XmlEvent::Text(text) => {
                if let Some(buffer) = pending_key.as_mut() {
                    buffer.push_str(text);
                }
            }
            XmlEvent::Close(element) => {
                if element == CompleteXmlElement::Key
                    && let Some(raw_key) = pending_key.take()
                    && !raw_key.is_empty()
                {
                    keys.push(raw_key);
                }
            }
            XmlEvent::End => break,
        }
    }
    Ok(keys)
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
    fn initiate_multipart_upload_result_xml_golden() {
        let result = InitiateMultipartUploadResult {
            bucket: "acme.models".to_owned(),
            key: "data/model.pt".to_owned(),
            upload_id: "upload-abc-123".to_owned(),
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Bucket>acme.models</Bucket>\n\
             \x20 <Key>data/model.pt</Key>\n\
             \x20 <UploadId>upload-abc-123</UploadId>\n\
             </InitiateMultipartUploadResult>\n"
        );
    }

    #[test]
    fn parse_complete_multipart_parts_extracts_numbers_in_order() {
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>\n\
             \x20 <Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part>\n\
             \x20 <Part><PartNumber>3</PartNumber><ETag>\"c\"</ETag></Part>\n\
             </CompleteMultipartUpload>\n";
        let parts = super::parse_complete_multipart_parts(body).unwrap();
        assert_eq!(parts.part_numbers(), &BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn parse_complete_multipart_parts_rejects_empty_or_malformed() {
        assert!(super::parse_complete_multipart_parts("").is_err());
        assert!(super::parse_complete_multipart_parts("<CompleteMultipartUpload/>").is_err());
        assert!(
            super::parse_complete_multipart_parts("<Part><PartNumber>0</PartNumber></Part>")
                .is_err()
        );
        assert!(
            super::parse_complete_multipart_parts("<Part><PartNumber>10001</PartNumber></Part>")
                .is_err()
        );
        assert!(
            super::parse_complete_multipart_parts(
                "<Part><PartNumber>not-a-number</PartNumber></Part>"
            )
            .is_err()
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

    #[test]
    fn parse_complete_multipart_parts_malformed_inputs() {
        // Truly truncated (no closing tag for the number).
        assert!(super::parse_complete_multipart_parts("<PartNumber>1").is_err());
        // Trailing truncated XML after a complete part is ignored.
        assert_eq!(
            super::parse_complete_multipart_parts("<PartNumber>1</PartNumber><Part><PartNumber>")
                .unwrap()
                .part_numbers(),
            &BTreeSet::from([1])
        );
        // Wrong casing is not matched.
        assert!(super::parse_complete_multipart_parts("<partnumber>1</partnumber>").is_err());
        // Entity-encoded numbers do not parse as integers.
        assert!(
            super::parse_complete_multipart_parts("<PartNumber>&lt;1&gt;</PartNumber>").is_err()
        );
        // Oversized numbers (u32 overflow and above the protocol cap).
        assert!(
            super::parse_complete_multipart_parts("<PartNumber>4294967296</PartNumber>").is_err()
        );
        assert!(super::parse_complete_multipart_parts("<PartNumber>10001</PartNumber>").is_err());
    }

    #[test]
    fn parse_complete_multipart_parts_huge_list_and_duplicates() {
        // A large-but-valid list parses in order.
        let body = (1..=5000)
            .map(|n| format!("<Part><PartNumber>{n}</PartNumber></Part>"))
            .collect::<String>();
        let parts = super::parse_complete_multipart_parts(&body).unwrap();
        assert_eq!(parts.len(), 5000);
        assert!(parts.part_numbers().contains(&1));
        assert!(parts.part_numbers().contains(&5000));
        // Duplicate part numbers are preserved in document order (the handler
        // validates the list against the stored session).
        let parts = super::parse_complete_multipart_parts(
            "<PartNumber>1</PartNumber><PartNumber>1</PartNumber>",
        )
        .unwrap();
        // Duplicate part numbers collapse into the set.
        assert_eq!(parts.part_numbers(), &BTreeSet::from([1]));
    }

    #[test]
    fn parse_complete_multipart_parts_ignores_etag_values() {
        // ETags are opaque and never validated by the parser — only the part
        // numbers matter. A wrong echoed ETag is accepted (the server
        // validates the part list against the stored session, not the ETag).
        let body = "<?xml version=\"1.0\"?><CompleteMultipartUpload>\
                    <Part><PartNumber>1</PartNumber><ETag>\"wrong\"</ETag></Part>\
                    <Part><PartNumber>2</PartNumber><ETag>\"wrong-again\"</ETag></Part>\
                    </CompleteMultipartUpload>";
        let parts = super::parse_complete_multipart_parts(body).unwrap();
        assert_eq!(parts.part_numbers(), &BTreeSet::from([1, 2]));
    }
    #[test]
    fn parse_delete_object_keys_extracts_keys_in_order() {
        let body = "<?xml version=\"1.0\"?><Delete>\
                    <Quiet>true</Quiet>\
                    <Object><Key>a.txt</Key></Object>\
                    <Object><Key>dir/b.txt</Key></Object>\
                    </Delete>";
        assert_eq!(
            super::parse_delete_object_keys(body).unwrap(),
            vec!["a.txt", "dir/b.txt"]
        );
    }

    #[test]
    fn parse_delete_object_keys_empty_and_malformed() {
        // Empty body / no keys.
        assert!(super::parse_delete_object_keys("").unwrap().is_empty());
        assert!(
            super::parse_delete_object_keys("<Delete></Delete>")
                .unwrap()
                .is_empty()
        );
        // An unterminated tag is an error; an unclosed `<Key>` (no closing
        // element) simply yields no key.
        assert!(super::parse_delete_object_keys("<Delete><Object><Key").is_err());
        assert!(
            super::parse_delete_object_keys("<Delete><Object><Key>a")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn list_all_my_buckets_result_xml_golden() {
        let result = ListBucketsResult {
            buckets: vec!["acme.models".to_owned()],
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <Buckets>\n\
             \x20\x20\x20\x20<Bucket><Name>acme.models</Name></Bucket>\n\
             \x20 </Buckets>\n\
             </ListAllMyBucketsResult>\n"
        );
    }

    #[test]
    fn copy_object_result_xml_golden() {
        let result = CopyObjectResult {
            etag: "ab12".to_owned(),
            last_modified_iso8601: "2026-08-13T09:51:00Z".to_owned(),
        };
        assert_eq!(
            result.to_xml(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             \x20 <ETag>\"ab12\"</ETag>\n\
             \x20 <LastModified>2026-08-13T09:51:00Z</LastModified>\n\
             </CopyObjectResult>\n"
        );
    }
}
