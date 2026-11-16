use thiserror::Error;

const MAX_OBJECT_KEY_BYTES: usize = 4096;
const MAX_OBJECT_PREFIX_BYTES: usize = 4096;

/// Validated object storage key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validates and creates an object key.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_storage::ObjectKey;
    ///
    /// let key = ObjectKey::parse("xorbs/default/aa/bb/example.xorb")?;
    /// assert_eq!(key.as_str(), "xorbs/default/aa/bb/example.xorb");
    ///
    /// // Absolute paths, traversal, and empty input are rejected.
    /// assert!(ObjectKey::parse("/etc/passwd").is_err());
    /// assert!(ObjectKey::parse("xorbs/../secret").is_err());
    /// assert!(ObjectKey::parse("").is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ObjectKeyError`] when the key is empty, too large, or contains path
    /// traversal, absolute path, backslash, or control-character input.
    pub fn parse(value: &str) -> Result<Self, ObjectKeyError> {
        if value.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        if value.len() > MAX_OBJECT_KEY_BYTES {
            return Err(ObjectKeyError::TooLong);
        }

        if !is_valid_storage_path(value, false) {
            return Err(ObjectKeyError::UnsafePath);
        }

        if value.chars().any(char::is_control) {
            return Err(ObjectKeyError::ControlCharacter);
        }

        Ok(Self(value.to_owned()))
    }

    /// Returns the validated key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated object storage prefix for inventory listing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPrefix(String);

impl ObjectPrefix {
    /// Validates and creates an object prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_storage::ObjectPrefix;
    ///
    /// let prefix = ObjectPrefix::parse("xorbs/default/")?;
    /// assert_eq!(prefix.as_str(), "xorbs/default/");
    ///
    /// // An empty prefix (the whole namespace) is valid.
    /// assert!(ObjectPrefix::parse("").is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ObjectPrefixError`] when the prefix is too large or contains path
    /// traversal, absolute path, backslash, or control-character input.
    pub fn parse(value: &str) -> Result<Self, ObjectPrefixError> {
        if value.len() > MAX_OBJECT_PREFIX_BYTES {
            return Err(ObjectPrefixError::TooLong);
        }

        if !is_valid_storage_path(value, true) {
            return Err(ObjectPrefixError::UnsafePath);
        }

        if value.chars().any(char::is_control) {
            return Err(ObjectPrefixError::ControlCharacter);
        }

        Ok(Self(value.to_owned()))
    }

    /// Returns the validated prefix as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Object key validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ObjectKeyError {
    /// The key was empty.
    #[error("object key must not be empty")]
    Empty,
    /// The key contained an unsafe path component.
    #[error("object key must be relative and must not contain traversal components")]
    UnsafePath,
    /// The key contained a control character.
    #[error("object key must not contain control characters")]
    ControlCharacter,
    /// The key exceeded the supported metadata bound.
    #[error("object key exceeded supported length")]
    TooLong,
}

/// Object prefix validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ObjectPrefixError {
    /// The prefix contained an unsafe path component.
    #[error("object prefix must be relative and must not contain traversal components")]
    UnsafePath,
    /// The prefix contained a control character.
    #[error("object prefix must not contain control characters")]
    ControlCharacter,
    /// The prefix exceeded the supported metadata bound.
    #[error("object prefix exceeded supported length")]
    TooLong,
}

fn is_valid_storage_path(value: &str, allow_trailing_separator: bool) -> bool {
    if value.starts_with('/') || value.contains('\\') {
        return false;
    }

    let trimmed = if allow_trailing_separator {
        value.strip_suffix('/').unwrap_or(value)
    } else {
        value
    };
    if trimmed.is_empty() {
        return allow_trailing_separator && value.is_empty();
    }

    trimmed
        .split('/')
        .all(|segment| !matches!(segment, "" | "." | ".."))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{
        MAX_OBJECT_KEY_BYTES, MAX_OBJECT_PREFIX_BYTES, ObjectKey, ObjectKeyError, ObjectPrefix,
        ObjectPrefixError,
    };

    #[test]
    fn object_key_cases_preserve_values_and_error_classification() {
        let oversized = "k".repeat(MAX_OBJECT_KEY_BYTES + 1);
        for (input, error) in [
            ("xorbs/default/aa/bb/hash.xorb", None),
            ("file.xorb", None),
            ("", Some(ObjectKeyError::Empty)),
            ("xorbs/../secret", Some(ObjectKeyError::UnsafePath)),
            ("/xorbs/hash", Some(ObjectKeyError::UnsafePath)),
            ("xorbs\\hash", Some(ObjectKeyError::UnsafePath)),
            ("xorbs/hash\n", Some(ObjectKeyError::ControlCharacter)),
            ("xorbs/./hash", Some(ObjectKeyError::UnsafePath)),
            ("xorbs//hash", Some(ObjectKeyError::UnsafePath)),
            ("xorbs/hash/", Some(ObjectKeyError::UnsafePath)),
            ("/", Some(ObjectKeyError::UnsafePath)),
            (oversized.as_str(), Some(ObjectKeyError::TooLong)),
            ("/bad\n", Some(ObjectKeyError::UnsafePath)),
        ] {
            assert_eq!(
                ObjectKey::parse(input).map(|key| key.as_str().to_owned()),
                error.map_or_else(|| Ok(input.to_owned()), Err),
                "{input:?}"
            );
        }
    }

    #[test]
    fn object_prefix_cases_preserve_values_and_error_classification() {
        let oversized = "p".repeat(MAX_OBJECT_PREFIX_BYTES + 1);
        for (input, error) in [
            ("", None),
            ("xorbs/default/", None),
            ("xorbs/../secret", Some(ObjectPrefixError::UnsafePath)),
            ("/xorbs/default/", Some(ObjectPrefixError::UnsafePath)),
            ("xorbs\\default\\", Some(ObjectPrefixError::UnsafePath)),
            (
                "xorbs/default/\n",
                Some(ObjectPrefixError::ControlCharacter),
            ),
            ("xorbs/./default/", Some(ObjectPrefixError::UnsafePath)),
            ("xorbs//default/", Some(ObjectPrefixError::UnsafePath)),
            ("/", Some(ObjectPrefixError::UnsafePath)),
            (oversized.as_str(), Some(ObjectPrefixError::TooLong)),
            ("/bad\n", Some(ObjectPrefixError::UnsafePath)),
        ] {
            assert_eq!(
                ObjectPrefix::parse(input).map(|prefix| prefix.as_str().to_owned()),
                error.map_or_else(|| Ok(input.to_owned()), Err),
                "{input:?}"
            );
        }
    }

    #[test]
    fn key_and_prefix_limits_count_bytes_and_precede_path_errors() {
        for input in ["a".repeat(4096), "é".repeat(2048)] {
            assert!(ObjectKey::parse(&input).is_ok());
            assert!(ObjectPrefix::parse(&input).is_ok());
            let oversized = format!("{input}/\n");
            assert_eq!(ObjectKey::parse(&oversized), Err(ObjectKeyError::TooLong));
            assert_eq!(
                ObjectPrefix::parse(&oversized),
                Err(ObjectPrefixError::TooLong)
            );
        }
    }

    proptest::proptest! {
        #[test]
        fn object_key_parse_never_accepts_absolute_path(s in ".*") {
            let input = format!("/{s}");
            let result = ObjectKey::parse(&input);
            prop_assert!(result.is_err(), "absolute path should be rejected: {input:?}");
        }

        #[test]
        fn object_key_parse_never_accepts_traversal(s in "[a-z]{1,10}") {
            let input = format!("{s}/../{s}");
            let result = ObjectKey::parse(&input);
            prop_assert!(result.is_err(), "traversal should be rejected: {input:?}");
        }

        #[test]
        fn object_key_parse_never_accepts_backslash(s in "[a-z]{1,10}") {
            let input = format!("{s}\\{s}");
            let result = ObjectKey::parse(&input);
            prop_assert!(result.is_err(), "backslash should be rejected: {input:?}");
        }

        #[test]
        fn object_key_parse_accepts_valid_relative_path(segs in prop::collection::vec("[a-z]{1,20}", 1..5usize)) {
            let input = segs.join("/");
            let result = ObjectKey::parse(&input);
            prop_assert!(result.is_ok(), "valid relative path should be accepted: {input:?}");
            let key = result.unwrap();
            prop_assert_eq!(key.as_str(), input.as_str());
        }

        #[test]
        fn object_key_parse_rejects_oversized(s in "[a-z]{4097,8192}") {
            let result = ObjectKey::parse(&s);
            prop_assert_eq!(result, Err(ObjectKeyError::TooLong));
        }

        #[test]
        fn object_key_parse_rejects_control_characters(s in "[a-z\t\n]{1,100}") {
            prop_assume!(s.chars().any(|c| c.is_control()));
            let result = ObjectKey::parse(&s);
            prop_assert!(result.is_err(), "control characters should be rejected: {s:?}");
        }
    }
}
