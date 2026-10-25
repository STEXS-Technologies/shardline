//! Byte-oriented HTTP entity-tag list comparison (RFC 9110, sections 8.8.3/13.1).

/// Tests one conditional field value against an existing representation's tag.
///
/// Preserves opaque bytes and commas inside tags. `weak` permits weak comparison;
/// otherwise both tags must be strong. Malformed lists do not match. A wildcard
/// matches an existing representation, so callers must establish existence first.
#[must_use]
pub fn entity_tag_list_matches(value: &[u8], current: &[u8], weak: bool) -> bool {
    compare_list(value, current, weak).unwrap_or(false)
}

/// Validated conditional entity-tag field, preserving every opaque wire byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityTagList(Vec<u8>);

impl EntityTagList {
    /// Parses a complete field value, rejecting malformed or empty lists.
    #[must_use]
    pub fn parse(value: &[u8]) -> Option<Self> {
        compare_list(value, b"", false)?;
        Some(Self(value.to_vec()))
    }

    /// Whether the field is the wildcard for an existing representation.
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        trim_ows(&self.0) == b"*"
    }

    /// Compares a quoted current tag, using strong or weak comparison.
    #[must_use]
    pub fn matches(&self, current: &[u8], weak: bool) -> bool {
        entity_tag_list_matches(&self.0, current, weak)
    }
}

fn compare_list(value: &[u8], current: &[u8], weak: bool) -> Option<bool> {
    let mut remaining = trim_ows(value);
    if remaining == b"*" {
        return Some(true);
    }
    let mut matched = false;
    let mut seen = false;
    while !remaining.is_empty() {
        // HTTP list recipients tolerate empty members.
        if let Some(rest) = remaining.strip_prefix(b",") {
            remaining = trim_ows(rest);
            continue;
        }
        let (is_weak, tag) = remaining
            .strip_prefix(b"W/")
            .map_or((false, remaining), |rest| (true, rest));
        let inside = tag.strip_prefix(b"\"")?;
        let end = inside.iter().position(|byte| *byte == b'"')?;
        let (opaque, tail) = inside.split_at(end);
        if !opaque
            .iter()
            .all(|byte| matches!(*byte, 0x21 | 0x23..=0x7e | 0x80..=0xff))
        {
            return None;
        }
        // Include both quotes, without interpreting backslashes as escapes.
        let quoted_length = end.checked_add(2)?;
        let (quoted, _) = tag.split_at(quoted_length);
        let current_weak = current.starts_with(b"W/");
        let current_tag = current.strip_prefix(b"W/").unwrap_or(current);
        matched |= (weak || (!is_weak && !current_weak)) && quoted == current_tag;
        seen = true;
        let (_, tail) = tail.split_at(1);
        remaining = trim_ows(tail);
        if !remaining.is_empty() && !remaining.starts_with(b",") {
            return None;
        }
    }
    seen.then_some(matched)
}

fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t'))
        .unwrap_or(value.len());
    let (_, value) = value.split_at(start);
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t'))
        .map_or(0, |index| index.saturating_add(1));
    value.split_at(end).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_bytes_commas_and_weak_comparison() {
        assert!(entity_tag_list_matches(
            b"\"\x80,other\", \"hash\"",
            b"\"hash\"",
            false
        ));
        assert!(entity_tag_list_matches(
            b"W/\"\x80,tag\"",
            b"\"\x80,tag\"",
            true
        ));
        assert!(!entity_tag_list_matches(b"W/\"hash\"", b"\"hash\"", false));
        assert!(!entity_tag_list_matches(b"\"hash\"", b"W/\"hash\"", false));
        assert!(entity_tag_list_matches(
            b", ,\t\"hash\",",
            b"\"hash\"",
            false
        ));
        assert!(entity_tag_list_matches(b" *\t", b"\"hash\"", false));
    }

    #[test]
    fn malformed_lists_never_authorize_a_match() {
        for value in [
            b"\"hash\", *".as_slice(),
            b"\"hash\" garbage",
            b"\"hash\", \"unfinished",
            b"\"bad tag\", \"hash\"",
            b"\"hash\"\x0b",
            b"w/\"hash\"",
        ] {
            assert!(!entity_tag_list_matches(value, b"\"hash\"", true));
        }
        assert!(!entity_tag_list_matches(
            b"\"other,hash\"",
            b"\"hash\"",
            true
        ));
    }
}
