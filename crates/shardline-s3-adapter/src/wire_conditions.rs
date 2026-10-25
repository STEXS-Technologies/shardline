//! Lossless conditional fields for HTTP frontends. The legacy string model
//! remains available to existing library callers.

use axum::http::{HeaderMap, header};
use shardline_protocol::EntityTagList;

use crate::InvalidS3HeaderValue;

/// A validated S3 precondition retaining opaque entity-tag bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConditionalHeader {
    is_match: bool,
    tags: EntityTagList,
}

impl WireConditionalHeader {
    /// Whether this is If-Match rather than If-None-Match.
    #[must_use]
    pub const fn is_match(&self) -> bool {
        self.is_match
    }

    /// Whether the condition uses a wildcard instead of concrete tags.
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.tags.is_wildcard()
    }

    /// Evaluates against the unquoted stored ETag; None means no object exists.
    #[must_use]
    pub fn satisfied(&self, stored_etag: Option<&str>) -> bool {
        let matched = stored_etag.is_some_and(|etag| {
            self.tags
                .matches(format!("\"{etag}\"").as_bytes(), !self.is_match)
        });
        if self.is_match { matched } else { !matched }
    }
}

/// Parses all repeated conditional fields, preserving If-Match precedence.
///
/// # Errors
/// Rejects malformed fields and wildcard/list mixtures before any mutation.
pub fn read_wire_conditional_headers(
    headers: &HeaderMap,
) -> Result<Vec<WireConditionalHeader>, InvalidS3HeaderValue> {
    let mut result = Vec::new();
    for (name, is_match) in [(header::IF_MATCH, true), (header::IF_NONE_MATCH, false)] {
        let mut values = headers.get_all(name).iter();
        let Some(first) = values.next() else {
            continue;
        };
        let mut combined = first.as_bytes().to_vec();
        for value in values {
            combined.push(b',');
            combined.extend_from_slice(value.as_bytes());
        }
        let tags = EntityTagList::parse(&combined).ok_or(InvalidS3HeaderValue)?;
        result.push(WireConditionalHeader { is_match, tags });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn opaque_lists_retain_comparison_and_existence_rules() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_MATCH,
            HeaderValue::from_bytes(b"\"\x80,other\", \"hash\"").unwrap(),
        );
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_bytes(b"W/\"\x80\", W/\"hash\"").unwrap(),
        );
        let conditions = read_wire_conditional_headers(&headers).unwrap();
        assert!(conditions[0].satisfied(Some("hash")));
        assert!(!conditions[0].satisfied(None));
        assert!(!conditions[1].satisfied(Some("hash")));
        assert!(conditions[1].satisfied(None));
        headers.append(header::IF_MATCH, "*".parse().unwrap());
        assert!(read_wire_conditional_headers(&headers).is_err());
    }
}
