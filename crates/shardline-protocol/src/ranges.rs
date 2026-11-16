use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Inclusive byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ByteRange {
    start: u64,
    end_inclusive: u64,
}

impl<'de> Deserialize<'de> for ByteRange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "ByteRange")]
        struct Fields {
            start: u64,
            end_inclusive: u64,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.start, fields.end_inclusive).map_err(serde::de::Error::custom)
    }
}

impl ByteRange {
    /// Creates an inclusive byte range.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::Inverted`] when `end_inclusive` is smaller than `start`.
    pub const fn new(start: u64, end_inclusive: u64) -> Result<Self, RangeError> {
        if end_inclusive < start {
            return Err(RangeError::Inverted);
        }

        Ok(Self {
            start,
            end_inclusive,
        })
    }

    /// Returns the first byte offset in the range.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Returns the inclusive final byte offset in the range.
    #[must_use]
    pub const fn end_inclusive(&self) -> u64 {
        self.end_inclusive
    }

    /// Returns the number of bytes in the range.
    #[must_use]
    pub const fn len(&self) -> Option<u64> {
        self.end_inclusive.saturating_sub(self.start).checked_add(1)
    }

    /// Returns false because validated inclusive byte ranges always contain at least one byte.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

/// End-exclusive chunk index range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ChunkRange {
    start: u32,
    end_exclusive: u32,
}

impl<'de> Deserialize<'de> for ChunkRange {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "ChunkRange")]
        struct Fields {
            start: u32,
            end_exclusive: u32,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.start, fields.end_exclusive).map_err(serde::de::Error::custom)
    }
}

impl ChunkRange {
    /// Creates an end-exclusive chunk index range.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::Empty`] when `end_exclusive` is equal to `start`.
    /// Returns [`RangeError::Inverted`] when `end_exclusive` is smaller than `start`.
    pub const fn new(start: u32, end_exclusive: u32) -> Result<Self, RangeError> {
        if end_exclusive < start {
            return Err(RangeError::Inverted);
        }

        if end_exclusive == start {
            return Err(RangeError::Empty);
        }

        Ok(Self {
            start,
            end_exclusive,
        })
    }

    /// Returns the first chunk index in the range.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Returns the end-exclusive chunk index.
    #[must_use]
    pub const fn end_exclusive(self) -> u32 {
        self.end_exclusive
    }
}

/// Range construction failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum RangeError {
    /// The range end was smaller than the range start.
    #[error("range end must not be smaller than range start")]
    Inverted,
    /// The range contained no chunks.
    #[error("chunk range must contain at least one chunk")]
    Empty,
}

/// Reconstruction request range parse failure.
#[derive(Debug, Clone, Error)]
pub enum HttpRangeParseError {
    /// The header did not start with the expected unit token.
    #[error("range header must use bytes=<start>-<end> syntax")]
    MissingBytesUnit,
    /// The header contained unsupported or malformed syntax.
    #[error("range header must use bytes=<start>-<end> syntax: {0}")]
    InvalidSyntax(String),
    /// The numeric range could not be parsed.
    #[error("range header contained an invalid number: {0}")]
    InvalidNumber(String),
    /// The requested start exceeded the represented resource length.
    #[error("requested range is not satisfiable")]
    Unsatisfiable,
}

/// Parses a reconstruction `Range` header into an inclusive byte range.
///
/// The Xet reconstruction API uses `bytes=<start>-<end>` syntax with an inclusive end.
/// When the requested end exceeds the resource length, the returned range is clamped to
/// the last byte of the resource.
///
/// # Errors
///
/// Returns [`HttpRangeParseError::InvalidSyntax`] when the header uses unsupported
/// syntax, [`HttpRangeParseError::InvalidNumber`] when parsing fails, and
/// [`HttpRangeParseError::Unsatisfiable`] when the requested start exceeds the last byte
/// of the resource.
pub fn parse_http_byte_range(
    value: &str,
    resource_length: u64,
) -> Result<ByteRange, HttpRangeParseError> {
    let Some(raw_suffix) = value.strip_prefix("bytes=") else {
        return Err(HttpRangeParseError::MissingBytesUnit);
    };
    if raw_suffix.is_empty() {
        return Err(HttpRangeParseError::InvalidSyntax(
            "empty range suffix".to_owned(),
        ));
    }
    if raw_suffix.contains(',') {
        return Err(HttpRangeParseError::InvalidSyntax(
            "multi-range not supported".to_owned(),
        ));
    }

    let Some((raw_start, raw_end)) = raw_suffix.split_once('-') else {
        return Err(HttpRangeParseError::InvalidSyntax(
            "missing range end".to_owned(),
        ));
    };
    if raw_start.is_empty() {
        // Suffix range: bytes=-N (last N bytes)
        let suffix_len = raw_end
            .parse::<u64>()
            .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?;
        if suffix_len == 0 {
            return Err(HttpRangeParseError::InvalidSyntax(
                "suffix length must be non-zero".to_owned(),
            ));
        }
        // A zero-length resource cannot satisfy any range; this matches the
        // non-suffix branch, which rejects `start >= resource_length` (and
        // would previously let a zero-length resource report a bogus 1-byte
        // `[0,0]` suffix range that `read_range` could read past the end of).
        if resource_length == 0 {
            return Err(HttpRangeParseError::Unsatisfiable);
        }
        // Clamp the requested suffix length to the resource length so a `-N`
        // larger than the resource returns the full range `[0, len-1]`.
        let suffix_len = suffix_len.min(resource_length);
        // `resource_length` is non-zero here (guarded above) and `suffix_len`
        // is clamped to `resource_length`, so neither `saturating_sub` can
        // actually saturate: `start = len - suffix_len`, `end = len - 1`.
        let start = resource_length.saturating_sub(suffix_len);
        let end = resource_length.saturating_sub(1);
        return ByteRange::new(start, end)
            .map_err(|err| HttpRangeParseError::InvalidSyntax(err.to_string()));
    }

    let start = raw_start
        .parse::<u64>()
        .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?;
    if start >= resource_length {
        return Err(HttpRangeParseError::Unsatisfiable);
    }

    let last_byte = resource_length
        .checked_sub(1)
        .ok_or(HttpRangeParseError::Unsatisfiable)?;
    let parsed_end = if raw_end.is_empty() {
        last_byte
    } else {
        raw_end
            .parse::<u64>()
            .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?
    };
    let end_inclusive = parsed_end.min(last_byte);

    ByteRange::new(start, end_inclusive)
        .map_err(|err| HttpRangeParseError::InvalidSyntax(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, ChunkRange, HttpRangeParseError, RangeError, parse_http_byte_range};

    #[test]
    fn deserialized_ranges_enforce_constructor_invariants() {
        for value in [
            serde_json::json!({"start": 9, "end_inclusive": 3}),
            serde_json::json!({"start": u64::MAX, "end_inclusive": 0}),
        ] {
            assert!(serde_json::from_value::<ByteRange>(value).is_err());
        }
        for value in [
            serde_json::json!({"start": 9, "end_exclusive": 3}),
            serde_json::json!({"start": 3, "end_exclusive": 3}),
            serde_json::json!({"start": 0, "end_exclusive": 0}),
        ] {
            assert!(serde_json::from_value::<ChunkRange>(value).is_err());
        }
    }

    #[test]
    fn deserialized_ranges_preserve_wire_format_and_boundary_values() {
        for (start, end) in [(0, 0), (7, 12), (u64::MAX, u64::MAX), (0, u64::MAX)] {
            let value = serde_json::json!({"start": start, "end_inclusive": end});
            let expected = ByteRange::new(start, end).unwrap();
            let decoded = serde_json::from_value::<ByteRange>(value.clone()).unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        }
        for (start, end) in [(0, 1), (7, 12), (u32::MAX - 1, u32::MAX), (0, u32::MAX)] {
            let value = serde_json::json!({"start": start, "end_exclusive": end});
            let expected = ChunkRange::new(start, end).unwrap();
            let decoded = serde_json::from_value::<ChunkRange>(value.clone()).unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        }
    }

    #[test]
    fn byte_range_inclusive_boundaries() {
        for (start, end, length) in [
            (10, 20, Some(11)),
            (0, u64::MAX, None),
            (5, 5, Some(1)),
            (0, 0, Some(1)),
            (u64::MAX, u64::MAX, Some(1)),
        ] {
            let range = ByteRange::new(start, end).unwrap();
            assert_eq!((range.start(), range.end_inclusive()), (start, end));
            assert_eq!(range.len(), length, "{range:?}");
            assert!(!range.is_empty(), "{range:?}");
        }
    }

    #[test]
    fn byte_range_rejects_inverted_input() {
        let range = ByteRange::new(20, 10);

        assert_eq!(range, Err(RangeError::Inverted));
    }

    #[test]
    fn chunk_range_rejects_empty_ranges() {
        let range = ChunkRange::new(4, 4);

        assert_eq!(range, Err(RangeError::Empty));
    }

    #[test]
    fn chunk_range_rejects_inverted_ranges() {
        let range = ChunkRange::new(5, 4);

        assert_eq!(range, Err(RangeError::Inverted));
    }

    #[test]
    fn chunk_range_exclusive_boundaries() {
        for (start, end) in [(4, 9), (3, 4), (0, u32::MAX)] {
            let range = ChunkRange::new(start, end).unwrap();
            assert_eq!((range.start(), range.end_exclusive()), (start, end));
        }
    }

    #[test]
    fn http_byte_range_accepted_forms() {
        for (header, resource_length, start, end) in [
            ("bytes=10-20", 100, 10, 20),
            ("bytes=10-", 25, 10, 24),
            ("bytes=10-999", 25, 10, 24),
            ("bytes=-1", 10, 9, 9),
            ("bytes=-100", 50, 0, 49),
            ("bytes=-999", 100, 0, 99),
            ("bytes=-4", 20, 16, 19),
            ("bytes=-20", 20, 0, 19),
            ("bytes=-999", 20, 0, 19),
            ("bytes=-1", 1, 0, 0),
            ("bytes=0-0", 1, 0, 0),
            ("bytes=0-18446744073709551615", u64::MAX, 0, u64::MAX - 1),
        ] {
            let range = parse_http_byte_range(header, resource_length).unwrap();
            assert_eq!(
                (range.start(), range.end_inclusive()),
                (start, end),
                "{header:?}, resource length {resource_length}"
            );
        }
    }

    #[test]
    fn http_byte_range_rejects_invalid_syntax() {
        for (header, resource_length) in [
            ("bytes=1-2,4-5", 10),
            ("bytes=2-1", 10),
            ("bytes=0-0,1-1", 10),
            ("bytes=", 10),
            ("bytes=10", 10),
            ("bytes=-0", 10),
            ("bytes=-0", 0),
        ] {
            assert!(
                matches!(
                    parse_http_byte_range(header, resource_length),
                    Err(HttpRangeParseError::InvalidSyntax(_))
                ),
                "{header:?}, resource length {resource_length}"
            );
        }
    }

    #[test]
    fn http_byte_range_rejects_missing_bytes_unit() {
        assert!(matches!(
            parse_http_byte_range("items=0-1", 10),
            Err(HttpRangeParseError::MissingBytesUnit)
        ));
    }

    #[test]
    fn http_byte_range_rejects_invalid_numbers() {
        for (header, resource_length) in [
            ("bytes= 1-2", 10),
            ("bytes=1 -2", 10),
            ("bytes=-", 10),
            ("bytes=99999999999999999999-100000000000000000000", 100),
            ("bytes=-18446744073709551616", 10),
        ] {
            assert!(
                matches!(
                    parse_http_byte_range(header, resource_length),
                    Err(HttpRangeParseError::InvalidNumber(_))
                ),
                "{header:?}, resource length {resource_length}"
            );
        }
    }

    #[test]
    fn http_byte_range_rejects_unsatisfiable_ranges() {
        for (header, resource_length) in [
            ("bytes=10-20", 10),
            ("bytes=0-0", 0),
            ("bytes=5-10", 5),
            ("bytes=-1", 0),
            ("bytes=-5", 0),
            ("bytes=-999", 0),
        ] {
            assert!(
                matches!(
                    parse_http_byte_range(header, resource_length),
                    Err(HttpRangeParseError::Unsatisfiable)
                ),
                "{header:?}, resource length {resource_length}"
            );
        }
    }

    #[test]
    fn range_error_display() {
        for (error, message) in [
            (
                RangeError::Inverted,
                "range end must not be smaller than range start",
            ),
            (
                RangeError::Empty,
                "chunk range must contain at least one chunk",
            ),
        ] {
            assert_eq!(error.to_string(), message, "{error:?}");
        }
    }

    #[test]
    fn http_range_parse_error_display_all_variants() {
        for (error, message) in [
            (
                HttpRangeParseError::MissingBytesUnit,
                "range header must use bytes=<start>-<end> syntax",
            ),
            (
                HttpRangeParseError::InvalidSyntax("test".to_owned()),
                "range header must use bytes=<start>-<end> syntax: test",
            ),
            (
                HttpRangeParseError::InvalidNumber("test".to_owned()),
                "range header contained an invalid number: test",
            ),
            (
                HttpRangeParseError::Unsatisfiable,
                "requested range is not satisfiable",
            ),
        ] {
            assert_eq!(error.to_string(), message, "{error:?}");
        }
    }
}
