use thiserror::Error;

const MAX_OBJECT_KEY_BYTES: usize = 4096;
const MAX_OBJECT_PREFIX_BYTES: usize = 4096;

/// Validated object storage key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validates and creates an object key.
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

    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{
        MAX_OBJECT_KEY_BYTES, MAX_OBJECT_PREFIX_BYTES, ObjectKey, ObjectKeyError, ObjectPrefix,
        ObjectPrefixError,
    };

    #[test]
    fn object_key_accepts_relative_content_paths() {
        let key = ObjectKey::parse("xorbs/default/aa/bb/hash.xorb");

        assert!(key.is_ok());
        if let Ok(value) = key {
            assert_eq!(value.as_str(), "xorbs/default/aa/bb/hash.xorb");
        }
    }

    #[test]
    fn object_key_rejects_empty_input() {
        let key = ObjectKey::parse("");

        assert_eq!(key, Err(ObjectKeyError::Empty));
    }

    #[test]
    fn object_key_rejects_path_traversal() {
        let key = ObjectKey::parse("xorbs/../secret");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_key_rejects_absolute_paths() {
        let key = ObjectKey::parse("/xorbs/hash");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_key_rejects_backslashes() {
        let key = ObjectKey::parse("xorbs\\hash");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_key_rejects_control_characters() {
        let key = ObjectKey::parse("xorbs/hash\n");

        assert_eq!(key, Err(ObjectKeyError::ControlCharacter));
    }

    #[test]
    fn object_key_rejects_oversized_values() {
        let key = ObjectKey::parse(&"k".repeat(MAX_OBJECT_KEY_BYTES + 1));

        assert_eq!(key, Err(ObjectKeyError::TooLong));
    }

    #[test]
    fn object_key_rejects_dot_segments() {
        let key = ObjectKey::parse("xorbs/./hash");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_key_rejects_empty_segments() {
        let key = ObjectKey::parse("xorbs//hash");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_key_rejects_trailing_separator() {
        let key = ObjectKey::parse("xorbs/hash/");

        assert_eq!(key, Err(ObjectKeyError::UnsafePath));
    }

    #[test]
    fn object_prefix_accepts_empty_prefix() {
        let prefix = ObjectPrefix::parse("");

        assert!(prefix.is_ok());
        if let Ok(value) = prefix {
            assert_eq!(value.as_str(), "");
        }
    }

    #[test]
    fn object_prefix_accepts_relative_content_prefix() {
        let prefix = ObjectPrefix::parse("xorbs/default/");

        assert!(prefix.is_ok());
        if let Ok(value) = prefix {
            assert_eq!(value.as_str(), "xorbs/default/");
        }
    }

    #[test]
    fn object_prefix_rejects_path_traversal() {
        let prefix = ObjectPrefix::parse("xorbs/../secret");

        assert_eq!(prefix, Err(ObjectPrefixError::UnsafePath));
    }

    #[test]
    fn object_prefix_rejects_absolute_paths() {
        let prefix = ObjectPrefix::parse("/xorbs/default/");

        assert_eq!(prefix, Err(ObjectPrefixError::UnsafePath));
    }

    #[test]
    fn object_prefix_rejects_backslashes() {
        let prefix = ObjectPrefix::parse("xorbs\\default\\");

        assert_eq!(prefix, Err(ObjectPrefixError::UnsafePath));
    }

    #[test]
    fn object_prefix_rejects_control_characters() {
        let prefix = ObjectPrefix::parse("xorbs/default/\n");

        assert_eq!(prefix, Err(ObjectPrefixError::ControlCharacter));
    }

    #[test]
    fn object_prefix_rejects_oversized_values() {
        let prefix = ObjectPrefix::parse(&"p".repeat(MAX_OBJECT_PREFIX_BYTES + 1));

        assert_eq!(prefix, Err(ObjectPrefixError::TooLong));
    }

    #[test]
    fn object_prefix_rejects_dot_segments() {
        let prefix = ObjectPrefix::parse("xorbs/./default/");

        assert_eq!(prefix, Err(ObjectPrefixError::UnsafePath));
    }

    #[test]
    fn object_prefix_rejects_empty_segments() {
        let prefix = ObjectPrefix::parse("xorbs//default/");

        assert_eq!(prefix, Err(ObjectPrefixError::UnsafePath));
    }

    #[test]
    fn object_key_error_display_empty() {
        assert_eq!(
            ObjectKeyError::Empty.to_string(),
            "object key must not be empty"
        );
    }

    #[test]
    fn object_key_error_display_unsafe_path() {
        assert_eq!(
            ObjectKeyError::UnsafePath.to_string(),
            "object key must be relative and must not contain traversal components"
        );
    }

    #[test]
    fn object_key_error_display_control_character() {
        assert_eq!(
            ObjectKeyError::ControlCharacter.to_string(),
            "object key must not contain control characters"
        );
    }

    #[test]
    fn object_key_error_display_too_long() {
        assert_eq!(
            ObjectKeyError::TooLong.to_string(),
            "object key exceeded supported length"
        );
    }

    #[test]
    fn object_prefix_error_display_unsafe_path() {
        assert_eq!(
            ObjectPrefixError::UnsafePath.to_string(),
            "object prefix must be relative and must not contain traversal components"
        );
    }

    #[test]
    fn object_prefix_error_display_control_character() {
        assert_eq!(
            ObjectPrefixError::ControlCharacter.to_string(),
            "object prefix must not contain control characters"
        );
    }

    #[test]
    fn object_prefix_error_display_too_long() {
        assert_eq!(
            ObjectPrefixError::TooLong.to_string(),
            "object prefix exceeded supported length"
        );
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
