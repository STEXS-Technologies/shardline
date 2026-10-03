use std::collections::{BTreeMap, BTreeSet, HashSet};

use axum::http::{
    HeaderMap, HeaderValue,
    header::InvalidHeaderValue,
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED},
};

use crate::protocol_support::etag_header;

/// Escapes XML text, omitting characters XML 1.0 cannot represent.
///
/// Carriage returns use a numeric reference so XML line-ending normalization
/// preserves the original text. Invalid control characters and U+FFFE/U+FFFF
/// are omitted, matching the S3 error envelope's existing sanitization policy.
#[must_use]
pub fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\r' => out.push_str("&#13;"),
            _ if !is_xml_character(u32::from(ch)) => {}
            _ => out.push(ch),
        }
    }
    out
}

const fn is_xml_character(code_point: u32) -> bool {
    matches!(code_point, 0x9 | 0xa | 0xd | 0x20..=0xd7ff | 0xe000..=0xfffd | 0x10000..=0x10ffff)
}

/// Decodes XML 1.0 numeric references and the five standard XML character references (`&amp;`, `&lt;`,
/// `&gt;`, `&quot;`, `&apos;`) in a `DeleteObjects` `<Key>` value.
///
/// S3 clients must XML-escape a key containing `&`, `<`, `>`, or quotes in the
/// request body (a stored key `a&b` arrives as `<Key>a&amp;b</Key>`). The
/// bounded scanner yields element text verbatim, so without decoding the key
/// would be looked up as the literal `a&amp;b`, never match the stored `a&b`,
/// and the response would report a false `<Deleted>` success for an object
/// that was not deleted (F-113).
///
/// A single left-to-right pass decodes each reference exactly once, so a
/// literal sequence like `&amp;lt;` (an escaped `&lt;` text) becomes `&lt;`
/// and is never double-decoded into `<`. Sequences that are not one of the
/// five named references or numeric references are left verbatim. The scanner
/// tolerates raw `&` text as before. Invalid numeric references return MalformedXML.
/// Numeric code points follow https://www.w3.org/TR/xml/#charsets.
fn decode_xml_entities(value: &str) -> Result<String, crate::S3Error> {
    if !value.contains('&') {
        return Ok(value.to_owned());
    }
    let mut decoded = String::with_capacity(value.len());
    let mut rest = value;
    loop {
        let Some(amp) = rest.find('&') else {
            decoded.push_str(rest);
            return Ok(decoded);
        };
        decoded.push_str(&rest[..amp]);
        let after_amp = &rest[amp.saturating_add(1)..];
        let Some(semi) = after_amp.find([';', '&']) else {
            if after_amp.starts_with('#') {
                return Err(crate::S3Error::malformed_xml());
            }
            // No terminating `;`: the `&` is literal trailing text.
            decoded.push('&');
            decoded.push_str(after_amp);
            return Ok(decoded);
        };
        if after_amp.as_bytes().get(semi) == Some(&b'&') {
            if after_amp.starts_with('#') {
                return Err(crate::S3Error::malformed_xml());
            }
            decoded.push('&');
            decoded.push_str(&after_amp[..semi]);
            rest = &after_amp[semi..];
            continue;
        }
        let name = &after_amp[..semi];
        let replacement = match name {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ if name.starts_with('#') => {
                let (digits, radix) = name
                    .strip_prefix("#x")
                    .map_or((&name[1..], 10), |hex| (hex, 16));
                if digits.is_empty()
                    || !digits.bytes().all(|byte| {
                        if radix == 16 {
                            byte.is_ascii_hexdigit()
                        } else {
                            byte.is_ascii_digit()
                        }
                    })
                {
                    return Err(crate::S3Error::malformed_xml());
                }
                let number = u32::from_str_radix(digits, radix)
                    .map_err(|_parse_error| crate::S3Error::malformed_xml())?;
                if !is_xml_character(number) {
                    return Err(crate::S3Error::malformed_xml());
                }
                Some(char::from_u32(number).ok_or_else(crate::S3Error::malformed_xml)?)
            }
            _ => None,
        };
        match replacement {
            Some(ch) => {
                decoded.push(ch);
                rest = &after_amp[semi.saturating_add(1)..];
            }
            None => {
                // Not a recognized reference: keep the `&` verbatim and keep
                // scanning after it so a later `&amp;` still decodes.
                decoded.push('&');
                rest = after_amp;
            }
        }
    }
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

/// Request values echoed by a ListObjectsV2 response. Filtering and cursors use
/// raw values; encoding applies only when serializing the response.
#[derive(Debug, Clone, Copy)]
pub struct ListObjectsV2ResponseContext<'request> {
    /// Whether key-like response fields use UTF-8 URL percent encoding.
    pub url_encoding: bool,
    /// Raw requested prefix.
    pub prefix: &'request str,
    /// Raw requested delimiter.
    pub delimiter: Option<&'request str>,
    /// Raw requested start-after key.
    pub start_after: Option<&'request str>,
}

fn listing_wire_value(value: &str, url_encoding: bool) -> String {
    if !url_encoding {
        return value.to_owned();
    }
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte))
            }
            _ => {
                encoded.push('%');
                // Both nibble conversions are bounded to 0..16.
                encoded.push(
                    char::from_digit(u32::from(byte >> 4), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                encoded.push(
                    char::from_digit(u32::from(byte & 15), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    encoded
}

fn encode_listing_rows(contents: &mut [Contents], prefixes: &mut [String]) {
    for entry in contents {
        entry.key = listing_wire_value(&entry.key, true);
    }
    for prefix in prefixes {
        *prefix = listing_wire_value(prefix, true);
    }
}

fn append_listing_fields(xml: &mut String, fields: &str) {
    if let Some(end) = xml.rfind("</ListBucketResult>") {
        xml.insert_str(end, fields);
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
    /// Serializes the listing with request echoes and optional lossless URL
    /// encoding. Opaque continuation tokens retain their original values.
    #[must_use]
    pub fn to_xml_with_context(&self, context: ListObjectsV2ResponseContext<'_>) -> String {
        let mut xml = if context.url_encoding {
            let mut result = self.clone();
            encode_listing_rows(&mut result.contents, &mut result.common_prefixes);
            result.to_xml()
        } else {
            self.to_xml()
        };
        let mut fields = format!(
            "  <Prefix>{}</Prefix>\n",
            xml_escape(&listing_wire_value(context.prefix, context.url_encoding))
        );
        for (tag, value) in [
            ("Delimiter", context.delimiter),
            ("StartAfter", context.start_after),
        ] {
            if let Some(value) = value {
                fields.push_str("  <");
                fields.push_str(tag);
                fields.push('>');
                fields.push_str(&xml_escape(&listing_wire_value(
                    value,
                    context.url_encoding,
                )));
                fields.push_str("</");
                fields.push_str(tag);
                fields.push_str(">\n");
            }
        }
        if context.url_encoding {
            fields.push_str("  <EncodingType>url</EncodingType>\n");
        }
        append_listing_fields(&mut xml, &fields);
        xml
    }

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
    /// Serializes the v1 response with optional lossless UTF-8 URL encoding of
    /// keys, prefix, delimiter and markers. Bucket names are unchanged.
    #[must_use]
    pub fn to_xml_with_url_encoding(&self, url_encoding: bool) -> String {
        if !url_encoding {
            return self.to_xml();
        }
        let mut result = self.clone();
        encode_listing_rows(&mut result.contents, &mut result.common_prefixes);
        result.prefix = listing_wire_value(&result.prefix, true);
        result.marker = listing_wire_value(&result.marker, true);
        result.delimiter = result
            .delimiter
            .map(|value| listing_wire_value(&value, true));
        result.next_marker = result
            .next_marker
            .map(|value| listing_wire_value(&value, true));
        let mut xml = result.to_xml();
        append_listing_fields(&mut xml, "  <EncodingType>url</EncodingType>\n");
        xml
    }

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
    /// The `PartNumber` element.
    PartNumber,
    /// An `ETag` element.
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
    /// A processing instruction or comment without application content.
    Ignored,
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
        if !is_closing && (name.starts_with('?') || name.starts_with('!')) {
            return Ok(XmlEvent::Ignored);
        }
        if is_closing {
            Ok(XmlEvent::Close(CompleteXmlElement::parse(name)))
        } else {
            Ok(XmlEvent::Open(CompleteXmlElement::parse(name)))
        }
    }
}

/// The parsed `CompleteMultipartUpload` request body.
///
/// Part numbers retain their strictly ascending request order and associated
/// echoed ETags. Missing tags are exposed as `None` for legacy API callers;
/// HTTP completion must reject them before consuming any part.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompleteParts {
    part_numbers: BTreeSet<u32>,
    etags: BTreeMap<u32, Option<String>>,
}

impl CompleteParts {
    /// Returns the part numbers in sorted order.
    #[must_use]
    pub const fn part_numbers(&self) -> &BTreeSet<u32> {
        &self.part_numbers
    }

    /// Returns the echoed ETag for a part, if supplied.
    #[must_use]
    pub fn etag(&self, part_number: u32) -> Option<&str> {
        self.etags.get(&part_number).and_then(Option::as_deref)
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

/// Parses ordered parts and their echoed ETags from completion XML.
///
/// Uses a bounded typed tokenizer. Each part must contain one valid part
/// number; duplicate or descending parts, duplicate fields, unmatched tags,
/// and unfinished fields are rejected. ETags decode the standard XML entities
/// once, so `&quot;` emitted by XML serializers retains its quoted value.
///
/// # Errors
///
/// Returns [`crate::S3Error::invalid_part`] for malformed input, invalid part
/// numbers, duplicate numbers or fields, or nonascending order.
pub fn parse_complete_multipart_parts(body: &str) -> Result<CompleteParts, crate::S3Error> {
    let mut scanner = CompleteXmlScanner::new(body);
    let mut parts = CompleteParts::default();
    let mut in_part = false;
    let mut root_open = false;
    let mut root_closed = false;
    let mut number = None;
    let mut etag = None;
    let mut field: Option<(CompleteXmlElement, String)> = None;
    loop {
        match scanner.next_event()? {
            XmlEvent::Ignored => {}
            XmlEvent::Open(CompleteXmlElement::CompleteMultipartUpload)
                if !root_open && !root_closed && !in_part && field.is_none() =>
            {
                root_open = true;
            }
            XmlEvent::Open(CompleteXmlElement::Part)
                if !in_part && field.is_none() && !root_closed =>
            {
                in_part = true;
                number = None;
                etag = None;
            }
            XmlEvent::Open(
                element @ (CompleteXmlElement::PartNumber | CompleteXmlElement::ETag),
            ) if in_part && field.is_none() => {
                if (element == CompleteXmlElement::PartNumber && number.is_some())
                    || (element == CompleteXmlElement::ETag && etag.is_some())
                {
                    return Err(crate::S3Error::invalid_part());
                }
                field = Some((element, String::new()));
            }
            XmlEvent::Text(text) => {
                if let Some((_element, buffer)) = field.as_mut() {
                    buffer.push_str(text);
                } else if !text.trim().is_empty() {
                    return Err(crate::S3Error::invalid_part());
                }
            }
            XmlEvent::Close(
                element @ (CompleteXmlElement::PartNumber | CompleteXmlElement::ETag),
            ) => {
                let (opened, text) = field.take().ok_or_else(crate::S3Error::invalid_part)?;
                if opened != element {
                    return Err(crate::S3Error::invalid_part());
                }
                if element == CompleteXmlElement::PartNumber {
                    let parsed = text
                        .trim()
                        .parse::<u32>()
                        .map_err(|_error| crate::S3Error::invalid_part())?;
                    if parsed == 0 || parsed > crate::multipart::MAX_S3_PART_NUMBER {
                        return Err(crate::S3Error::invalid_part());
                    }
                    number = Some(parsed);
                } else {
                    etag = Some(decode_xml_entities(text.trim())?);
                }
            }
            XmlEvent::Close(CompleteXmlElement::Part) if in_part && field.is_none() => {
                let number = number.take().ok_or_else(crate::S3Error::invalid_part)?;
                if parts.max_part().is_some_and(|previous| number <= previous) {
                    return Err(crate::S3Error::invalid_part());
                }
                parts.part_numbers.insert(number);
                parts.etags.insert(number, etag.take());
                in_part = false;
            }
            XmlEvent::Close(CompleteXmlElement::CompleteMultipartUpload)
                if root_open && !in_part && field.is_none() =>
            {
                root_open = false;
                root_closed = true;
            }
            XmlEvent::End => break,
            XmlEvent::Open(_) | XmlEvent::Close(_) => return Err(crate::S3Error::invalid_part()),
        }
    }
    if parts.is_empty() || in_part || root_open || field.is_some() {
        return Err(crate::S3Error::invalid_part());
    }
    Ok(parts)
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
/// element text is read (the `Quiet` flag is ignored); keys are returned in
/// first-occurrence document order with duplicates collapsed (the handler's
/// dedupe pass becomes a no-op over the returned list). Empty keys are skipped.
///
/// Each `<Key>` value is decoded through [`decode_xml_entities`] before it is
/// returned: S3 clients XML-escape keys containing `&`, `<`, `>`, or quotes
/// (a stored key `a&b` arrives as `<Key>a&amp;b</Key>`), and the decoded key
/// is what the handler looks up, deletes, and reports in the `<DeleteResult>`
/// — so a decoded key is never reported as a false `<Deleted>` success (F-113).
///
/// The [`MAX_S3_DELETE_KEYS`] protocol cap is enforced **during** parsing: the
/// first *distinct* key beyond the cap aborts with `MalformedXML` (`400`)
/// before it is ever buffered, so the returned list — and the dedupe set used
/// to enforce the cap — never grow beyond the cap. A hostile body of millions
/// of duplicate `<Key>` entries therefore consumes at most `cap` owned keys of
/// memory instead of amplifying every entry into an owned `String` (F-32);
/// the cap is only tripped by a genuine `cap + 1`-th distinct key. Dedupe and
/// the cap operate on the *decoded* keys, so two encodings of the same key
/// collapse into a single delete.
///
/// # Errors
///
/// Returns [`crate::S3Error::invalid_part`] when the body contains an
/// unterminated tag, or [`crate::S3Error::malformed_xml`] when the body lists
/// more than [`MAX_S3_DELETE_KEYS`] distinct keys or contains an invalid
/// numeric character reference.
pub fn parse_delete_object_keys(body: &str) -> Result<Vec<String>, crate::S3Error> {
    let mut scanner = CompleteXmlScanner::new(body);
    let mut keys = Vec::with_capacity(MAX_S3_DELETE_KEYS.min(32));
    let mut seen = HashSet::with_capacity(MAX_S3_DELETE_KEYS.min(32));
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
                    // The scanner yields text verbatim, so decode the standard
                    // XML character references before the key is looked up —
                    // otherwise an entity-encoded key never matches the stored
                    // object and the response would report a false `<Deleted>`
                    // success (F-113).
                    let key = decode_xml_entities(&raw_key)?;
                    // Dedupe while parsing: duplicates collapse to one key and
                    // never count toward the cap. A NEW key beyond the cap is
                    // rejected before it is buffered, bounding both `keys` and
                    // `seen` at the cap (F-32).
                    if !seen.insert(key.clone()) {
                        continue;
                    }
                    if keys.len() >= MAX_S3_DELETE_KEYS {
                        return Err(crate::S3Error::malformed_xml());
                    }
                    keys.push(key);
                }
            }
            XmlEvent::Ignored => {}
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
    fn xml_text_filters_invalid_scalars_and_preserves_carriage_returns() {
        let value = "a\r\nb\u{fffe}\u{ffff}\u{fffd}\u{10000}\u{10ffff}";
        let escaped = xml_escape(value);
        assert_eq!(escaped, "a&#13;\nb\u{fffd}\u{10000}\u{10ffff}");
        assert_eq!(
            decode_xml_entities(&escaped).unwrap(),
            "a\r\nb\u{fffd}\u{10000}\u{10ffff}"
        );
        let xml = S3ErrorBody {
            code: "NoSuchKey".to_owned(),
            message: value.to_owned(),
            key: Some(value.to_owned()),
            request_id: Some(value.to_owned()),
        }
        .to_xml();
        assert!(!xml.contains(['\u{fffe}', '\u{ffff}', '\r']));
        assert_eq!(xml.matches("&#13;").count(), 3);
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
        // Truncated trailing XML must not silently discard requested parts.
        assert!(
            super::parse_complete_multipart_parts(
                "<Part><PartNumber>1</PartNumber></Part><Part><PartNumber>"
            )
            .is_err()
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
        assert!(
            super::parse_complete_multipart_parts(
                "<Part><PartNumber>1</PartNumber></Part><Part><PartNumber>1</PartNumber></Part>",
            )
            .is_err()
        );
        assert!(
            super::parse_complete_multipart_parts(
                "<Part><PartNumber>2</PartNumber></Part><Part><PartNumber>1</PartNumber></Part>",
            )
            .is_err()
        );
    }

    #[test]
    fn parse_complete_multipart_parts_retains_etag_values() {
        // The server validates each echoed value against acknowledged content.
        let body = "<?xml version=\"1.0\"?><CompleteMultipartUpload>\
                    <Part><PartNumber>1</PartNumber><ETag>\"wrong\"</ETag></Part>\
                    <Part><PartNumber>2</PartNumber><ETag>\"wrong-again\"</ETag></Part>\
                    </CompleteMultipartUpload>";
        let parts = super::parse_complete_multipart_parts(body).unwrap();
        assert_eq!(parts.part_numbers(), &BTreeSet::from([1, 2]));
        assert_eq!(parts.etag(1), Some("\"wrong\""));
        assert_eq!(parts.etag(2), Some("\"wrong-again\""));
    }
    #[test]
    fn completion_etags_decode_once_and_reject_ambiguous_fields() {
        let parts = super::parse_complete_multipart_parts(
            "<Part><PartNumber>1</PartNumber><ETag>&quot;abc&quot;</ETag></Part>",
        )
        .unwrap();
        assert_eq!(parts.etag(1), Some("\"abc\""));
        let parts = super::parse_complete_multipart_parts(
            "<Part><PartNumber>1</PartNumber><ETag>&amp;quot;abc&amp;quot;</ETag></Part>",
        )
        .unwrap();
        assert_eq!(parts.etag(1), Some("&quot;abc&quot;"));
        for body in [
            "<Part><PartNumber>1</PartNumber><PartNumber>2</PartNumber></Part>",
            "<Part><PartNumber>1</PartNumber><ETag>a</ETag><ETag>b</ETag></Part>",
            "<Part><PartNumber>1</ETag></Part>",
            "<Part><PartNumber>1</PartNumber>",
            "<Part><ETag>a</ETag></Part>",
            "<PartNumber>1</PartNumber>",
            "<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part><Part><PartNumber>1</PartNumber><ETag>b</ETag></Part>",
        ] {
            assert!(
                super::parse_complete_multipart_parts(body).is_err(),
                "accepted {body}"
            );
        }
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
    fn parse_delete_object_keys_cap_enforced_on_distinct_keys_during_parse() {
        // Exactly MAX_S3_DELETE_KEYS distinct keys parse fine, in order.
        let objects = (0..super::MAX_S3_DELETE_KEYS)
            .map(|index| format!("<Object><Key>cap/key-{index:04}.txt</Key></Object>"))
            .collect::<String>();
        let mut body = format!("<Delete>{objects}</Delete>");
        let keys = super::parse_delete_object_keys(&body).unwrap();
        assert_eq!(keys.len(), super::MAX_S3_DELETE_KEYS);
        assert_eq!(keys[0], "cap/key-0000.txt");

        // The (MAX+1)-th DISTINCT key is rejected mid-parse with MalformedXML
        // (400) before it is ever buffered — the Vec never exceeds the cap.
        body.insert_str(
            body.len() - "</Delete>".len(),
            "<Object><Key>cap/overflow.txt</Key></Object>",
        );
        let error = super::parse_delete_object_keys(&body).unwrap_err();
        assert_eq!(error.code, "MalformedXML");
        assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn parse_delete_object_keys_duplicates_never_trip_the_cap() {
        // F-32: a hostile body of repeated duplicate keys must not amplify
        // memory — millions of identical entries collapse to a single key,
        // never growing the parsed list (or the dedupe set) beyond the cap.
        let duplicates = "<Object><Key>x</Key></Object>".repeat(super::MAX_S3_DELETE_KEYS + 10);
        let body = format!("<Delete>{duplicates}</Delete>");
        assert_eq!(
            super::parse_delete_object_keys(&body).unwrap(),
            vec!["x".to_owned()]
        );
    }

    #[test]
    fn parse_delete_object_keys_decodes_xml_entities() {
        // F-113: a client XML-escapes a key containing `&`, `<`, `>`, or
        // quotes in the body (a stored key `a&b` arrives as `<Key>a&amp;b</Key>`).
        // The parser must decode the five standard character references so the
        // decoded key is what gets looked up, deleted, and reported — never a
        // literal `a&amp;b` that matches nothing and yields a false success.
        let body = "<?xml version=\"1.0\"?><Delete>\
                    <Object><Key>a&amp;b</Key></Object>\
                    <Object><Key>c&lt;d</Key></Object>\
                    <Object><Key>e&gt;f</Key></Object>\
                    <Object><Key>g&quot;h</Key></Object>\
                    <Object><Key>i&apos;j</Key></Object>\
                    </Delete>";
        assert_eq!(
            super::parse_delete_object_keys(body).unwrap(),
            vec!["a&b", "c<d", "e>f", "g\"h", "i'j"]
        );
    }

    #[test]
    fn numeric_xml_references_decode_once_and_deduplicate() {
        let keys = parse_delete_object_keys("<Delete><Object><Key>a&#38;b</Key></Object><Object><Key>a&#x26;b</Key></Object><Object><Key>&#60;&#x1F600;</Key></Object><Object><Key>&amp;#38;</Key></Object></Delete>").unwrap();
        assert_eq!(keys, vec!["a&b", "<😀", "&#38;"]);
    }

    #[test]
    fn invalid_numeric_xml_references_are_rejected() {
        for reference in [
            "&#0;",
            "&#xD800;",
            "&#55296;",
            "&#x110000;",
            "&#4294967296;",
            "&#xFFFE;",
            "&#31;",
            "&#;",
            "&#x;",
            "&#+38;",
            "&#xGG;",
            "&#X26;",
            "&#38",
            "&#38&amp;",
        ] {
            let xml = format!("<Delete><Object><Key>{reference}</Key></Object></Delete>");
            assert!(parse_delete_object_keys(&xml).is_err(), "{reference}");
        }
    }

    #[test]
    fn parse_delete_object_keys_entity_decoding_is_single_pass() {
        // `&amp;lt;` is an escaped literal `&lt;` text: it must decode to the
        // two-character sequence `&lt;`, never be double-decoded into `<`.
        // Unknown `&…;` sequences are left verbatim, and a raw `&` without a
        // terminating `;` is literal text.
        let body = "<Delete>\
                    <Object><Key>a&amp;lt;b</Key></Object>\
                    <Object><Key>c&unknown;d</Key></Object>\
                    <Object><Key>e&f</Key></Object>\
                    </Delete>";
        assert_eq!(
            super::parse_delete_object_keys(body).unwrap(),
            vec!["a&lt;b", "c&unknown;d", "e&f"]
        );
    }

    #[test]
    fn parse_delete_object_keys_entity_encodings_of_one_key_collapse() {
        // Two encodings of the same stored key (`a&b` written both escaped and
        // raw) must collapse into a single delete — dedupe runs on the decoded
        // key, so the pair never counts twice toward the cap.
        let body = "<Delete>\
                    <Object><Key>a&amp;b</Key></Object>\
                    <Object><Key>a&b</Key></Object>\
                    </Delete>";
        assert_eq!(
            super::parse_delete_object_keys(body).unwrap(),
            vec!["a&b".to_owned()]
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

    #[test]
    fn list_bucket_v1_and_empty_bucket_xml_escape_fields() {
        let result = ListBucketResultV1 {
            contents: vec![Contents {
                key: "a&b".to_owned(),
                size_bytes: 7,
                etag: "deadbeef".to_owned(),
                last_modified_iso8601: "2026-01-01T00:00:00Z".to_owned(),
            }],
            common_prefixes: vec!["dir/".to_owned()],
            name: "bucket&name".to_owned(),
            prefix: "a<".to_owned(),
            marker: "m>".to_owned(),
            max_keys: 7,
            delimiter: Some("/".to_owned()),
            is_truncated: true,
            next_marker: Some("next&".to_owned()),
        };
        let xml = result.to_xml();
        assert!(xml.contains("<Name>bucket&amp;name</Name>"));
        assert!(xml.contains("<Prefix>a&lt;</Prefix>"));
        assert!(xml.contains("<NextMarker>next&amp;</NextMarker>"));
        let empty = ListBucketsResult {
            buckets: Vec::new(),
        }
        .to_xml();
        assert!(empty.contains("<Buckets>"));
        assert!(empty.contains("</Buckets>"));
    }
}
