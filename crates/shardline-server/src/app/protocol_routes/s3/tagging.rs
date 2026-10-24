//! S3 object tagging encoded inside the existing integrity-bound metadata row.

use std::collections::HashSet;

use axum::http::HeaderMap;
use serde::Deserialize;
use shardline_s3_adapter::{S3Error, xml_escape};

use super::object_headers::TAGGING_KEY;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaggingDocument {
    #[serde(rename = "@xmlns")]
    _namespace: Option<String>,
    #[serde(rename = "TagSet")]
    tag_set: TagSetDocument,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TagSetDocument {
    #[serde(rename = "Tag", default)]
    tags: Vec<TagDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TagDocument {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

/// Decodes and validates an S3 `PutObjectTagging` body.
pub(super) fn parse(bytes: &[u8]) -> Result<Vec<(String, String)>, S3Error> {
    let document: TaggingDocument =
        quick_xml::de::from_reader(bytes).map_err(|_error| S3Error::malformed_xml())?;
    let mut tags = document
        .tag_set
        .tags
        .into_iter()
        .map(|tag| (tag.key, tag.value))
        .collect::<Vec<_>>();
    validate(&tags)?;
    tags.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(tags)
}

/// Parses the URL-encoded `x-amz-tagging` header used by object and multipart
/// uploads. `None` means the request did not supply the header.
pub(super) fn parse_header(headers: &HeaderMap) -> Result<Option<Vec<(String, String)>>, S3Error> {
    let mut values = headers.get_all("x-amz-tagging").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(S3Error::invalid_argument("Repeated x-amz-tagging header"));
    }
    let value = value
        .to_str()
        .map_err(|_error| S3Error::invalid_argument("Invalid x-amz-tagging header"))?;
    let tags = url::form_urlencoded::parse(value.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    validate(&tags)?;
    let mut tags = tags;
    tags.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(Some(tags))
}

/// Reads the reserved tag entry from integrity-bound object metadata.
pub(super) fn from_metadata(
    metadata: &[(String, String)],
) -> Result<Vec<(String, String)>, S3Error> {
    let Some((_, json)) = metadata
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(TAGGING_KEY))
    else {
        return Ok(Vec::new());
    };
    let tags: Vec<(String, String)> =
        serde_json::from_str(json).map_err(|_error| S3Error::internal())?;
    validate(&tags).map_err(|_error| S3Error::internal())?;
    Ok(tags)
}

/// Replaces the reserved metadata entry. Empty tagging is represented by the
/// absence of the entry, preserving historical S3 evidence encodings.
pub(super) fn replace_metadata(
    metadata: &mut Vec<(String, String)>,
    tags: &[(String, String)],
) -> Result<(), S3Error> {
    validate(tags)?;
    metadata.retain(|(name, _)| !name.eq_ignore_ascii_case(TAGGING_KEY));
    if !tags.is_empty() {
        let mut tags = tags.to_vec();
        tags.sort_by(|left, right| left.0.cmp(&right.0));
        metadata.push((
            TAGGING_KEY.to_owned(),
            serde_json::to_string(&tags).map_err(|_error| S3Error::internal())?,
        ));
        metadata.sort_by(|left, right| left.0.cmp(&right.0));
    }
    Ok(())
}

pub(super) fn to_xml(tags: &[(String, String)]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>",
    );
    for (key, value) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&xml_escape(key));
        xml.push_str("</Key><Value>");
        xml.push_str(&xml_escape(value));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    xml
}

fn validate(tags: &[(String, String)]) -> Result<(), S3Error> {
    if tags.len() > 10 {
        return Err(S3Error::invalid_argument(
            "An object can have at most 10 tags",
        ));
    }
    let mut keys = HashSet::with_capacity(tags.len());
    for (key, value) in tags {
        if key.is_empty()
            || key.chars().count() > 128
            || value.chars().count() > 256
            || key.to_ascii_lowercase().starts_with("aws:")
            || !keys.insert(key)
        {
            return Err(S3Error::invalid_argument("Invalid S3 object tagging"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagging_round_trips_and_canonicalizes_order() {
        let parsed = parse(br#"<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>z</Key><Value>&amp;value</Value></Tag><Tag><Key>a</Key><Value>first</Value></Tag></TagSet></Tagging>"#).unwrap();
        assert_eq!(
            parsed,
            vec![("a".into(), "first".into()), ("z".into(), "&value".into())]
        );
        let xml = to_xml(&parsed);
        assert_eq!(parse(xml.as_bytes()).unwrap(), parsed);
    }

    #[test]
    fn tagging_rejects_duplicate_reserved_and_oversized_values() {
        for body in [
            r#"<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>"#,
            r#"<Tagging><TagSet><Tag><Key>aws:owned</Key><Value>1</Value></Tag></TagSet></Tagging>"#,
        ] {
            assert!(parse(body.as_bytes()).is_err());
        }
        assert!(parse(b"<Tagging>").is_err());
    }

    #[test]
    fn tagging_rejects_duplicate_attributes_even_after_many_namespaces() {
        assert!(parse(br#"<Tagging xmlns="one" xmlns="two"><TagSet/></Tagging>"#).is_err());
        let mut document = String::from("<Tagging");
        for index in 0..4096 {
            use std::fmt::Write as _;
            write!(document, " xmlns:p{index}=\"urn:tagging:{index}\"").unwrap();
        }
        document.push_str(" xmlns:p4095=\"urn:duplicate\"><TagSet/></Tagging>");
        assert!(parse(document.as_bytes()).is_err());
    }
}
