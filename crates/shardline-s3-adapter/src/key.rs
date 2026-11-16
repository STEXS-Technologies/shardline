use shardline_storage::{ObjectKey, ObjectKeyError};
use thiserror::Error;

/// Maximum S3 bucket name length, mirroring the S3 bucket-name limit applied
/// to the `{owner}.{name}` compound.
const MAX_BUCKET_NAME_BYTES: usize = 63;

/// S3 bucket decode failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BucketDecodeError {
    /// The bucket did not contain a `.` separating the owner from the name.
    #[error("bucket must contain a '.' separating owner from name")]
    MissingSeparator,
    /// The decoded owner contained a `.` and is therefore not addressable.
    #[error("bucket owner must not contain a '.'")]
    OwnerContainsDot,
    /// The bucket contained an uppercase character.
    #[error("bucket name must be lowercase")]
    NotLowercase,
    /// The bucket exceeded the S3 63-character limit.
    #[error("bucket name must not exceed 63 characters")]
    TooLong,
    /// The bucket owner or name was empty.
    #[error("bucket owner and name must be non-empty")]
    EmptyPart,
}

/// Encodes a repository owner and name into the `{owner}.{name}` S3 bucket.
///
/// # Examples
///
/// ```
/// use shardline_s3_adapter::encode_bucket;
///
/// assert_eq!(encode_bucket("acme", "models"), "acme.models");
/// ```
#[must_use]
pub fn encode_bucket(owner: &str, name: &str) -> String {
    format!("{owner}.{name}")
}

/// Decodes a `{owner}.{name}` S3 bucket into its parts.
///
/// The bucket is split on the *first* `.` (`a.b.c` decodes to owner `a` and
/// name `b.c`). An owner containing a `.` cannot be represented by this layout.
///
/// # Examples
///
/// ```
/// use shardline_s3_adapter::decode_bucket;
///
/// assert_eq!(
///     decode_bucket("acme.models")?,
///     ("acme".to_owned(), "models".to_owned())
/// );
/// assert_eq!(
///     decode_bucket("a.b.c")?,
///     ("a".to_owned(), "b.c".to_owned())
/// );
/// assert!(decode_bucket("acme").is_err());
/// assert!(decode_bucket("Acme.models").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`BucketDecodeError`] when the bucket is missing the `.` separator,
/// the bucket is not lowercase or exceeds
/// 63 characters, or the owner or name is empty.
pub fn decode_bucket(bucket: &str) -> Result<(String, String), BucketDecodeError> {
    let Some((owner, name)) = bucket.split_once('.') else {
        return Err(BucketDecodeError::MissingSeparator);
    };
    if owner.is_empty() || name.is_empty() {
        return Err(BucketDecodeError::EmptyPart);
    }
    if bucket.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(BucketDecodeError::NotLowercase);
    }
    if bucket.len() > MAX_BUCKET_NAME_BYTES {
        return Err(BucketDecodeError::TooLong);
    }
    Ok((owner.to_owned(), name.to_owned()))
}

/// S3 object key construction failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum S3KeyError {
    /// The object key was empty.
    #[error("s3 object key must not be empty")]
    EmptyKey,
    /// The object key started with a leading slash.
    #[error("s3 object key must not start with '/'")]
    LeadingSlash,
    /// The object key contained a control character.
    #[error("s3 object key must not contain control characters")]
    ControlCharacter,
    /// The assembled object key exceeded the storage key limits.
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
}

/// A validated client-facing S3 object key.
///
/// Both constructors share the key-level validation (non-empty, no leading
/// slash, no control characters); the assembled
/// storage-key length and path-safety checks remain in
/// [`s3_object_key`]'s `ObjectKey::parse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectKey(String);

impl S3ObjectKey {
    /// Validates a client-facing S3 object key.
    ///
    /// # Errors
    ///
    /// Returns [`S3KeyError::EmptyKey`] for an empty key,
    /// [`S3KeyError::LeadingSlash`] when the key starts with `/`, and
    /// [`S3KeyError::ControlCharacter`] when it contains a control character.
    pub fn parse(key: &str) -> Result<Self, S3KeyError> {
        Self::validate(key)?;
        Ok(Self(key.to_owned()))
    }

    fn validate(key: &str) -> Result<(), S3KeyError> {
        if key.is_empty() {
            return Err(S3KeyError::EmptyKey);
        }
        if key.as_bytes().first() == Some(&b'/') {
            return Err(S3KeyError::LeadingSlash);
        }
        if key.chars().any(char::is_control) {
            return Err(S3KeyError::ControlCharacter);
        }
        Ok(())
    }

    /// Returns the validated key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Builds the storage object key for an S3 object.
///
/// The key layout is `protocols/s3/{scope_namespace}/{key}` where
/// `scope_namespace` is the sha256 repository-scope namespace produced by
/// `shardline_server_core::protocol_support::scope_namespace`.
///
/// # Examples
///
/// ```
/// use shardline_s3_adapter::s3_object_key;
///
/// let key = s3_object_key("global", "data/model.pt")?;
/// assert_eq!(key.as_str(), "protocols/s3/global/data/model.pt");
/// assert!(s3_object_key("global", "/leading-slash").is_err());
/// assert!(s3_object_key("global", "").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`S3KeyError`] when the key is empty, starts with `/`, contains a
/// control character, or the assembled path exceeds the [`ObjectKey`] limits.
pub fn s3_object_key(scope_namespace: &str, key: &str) -> Result<ObjectKey, S3KeyError> {
    S3ObjectKey::validate(key)?;
    ObjectKey::parse(&format!("protocols/s3/{scope_namespace}/{key}")).map_err(S3KeyError::from)
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
    fn bucket_encode_decode_roundtrip() {
        assert_eq!(encode_bucket("acme", "models"), "acme.models");
        // Decoding splits on the first dot; remaining dots belong to the name.
        // Unicode is allowed because only ASCII uppercase is rejected.
        for (bucket, owner, name) in [
            ("acme.models", "acme", "models"),
            ("a.b.c", "a", "b.c"),
            ("ac.me.models", "ac", "me.models"),
            ("a.b.c.d", "a", "b.c.d"),
            ("a..b", "a", ".b"),
            ("über.models", "über", "models"),
        ] {
            assert_eq!(
                decode_bucket(bucket).unwrap(),
                (owner.to_owned(), name.to_owned()),
                "{bucket}"
            );
            assert_eq!(encode_bucket(owner, name), bucket);
        }
    }

    #[test]
    fn bucket_decode_error_contract() {
        for (bucket, error) in [
            ("acme", BucketDecodeError::MissingSeparator),
            ("", BucketDecodeError::MissingSeparator),
            ("Acme.models", BucketDecodeError::NotLowercase),
            ("acme.Models", BucketDecodeError::NotLowercase),
            (".models", BucketDecodeError::EmptyPart),
            ("acme.", BucketDecodeError::EmptyPart),
            ("A.B", BucketDecodeError::NotLowercase),
            (".b", BucketDecodeError::EmptyPart),
            ("a.", BucketDecodeError::EmptyPart),
        ] {
            assert_eq!(decode_bucket(bucket), Err(error), "{bucket:?}");
        }
    }

    #[test]
    fn bucket_decode_dotted_owner_error_message() {
        // Split-on-first-dot prevents this variant during decoding, but its
        // public message remains part of the bucket error contract.
        let err = BucketDecodeError::OwnerContainsDot;
        assert_eq!(err.to_string(), "bucket owner must not contain a '.'");
        let _ = err;
    }

    #[test]
    fn bucket_decode_rejects_oversized() {
        let bucket = format!("{}.{}", "a".repeat(63), "b".repeat(64));
        assert!(matches!(
            decode_bucket(&bucket),
            Err(BucketDecodeError::TooLong)
        ));
        assert!(matches!(
            decode_bucket(&format!("{}.{}", "a".repeat(32), "b".repeat(31))),
            Err(BucketDecodeError::TooLong)
        ));
        // 63 characters total (including the separator) is accepted.
        assert!(decode_bucket(&format!("{}.{}", "a".repeat(31), "b".repeat(31))).is_ok());
    }

    #[test]
    fn s3_object_key_layout() {
        let key = s3_object_key("global", "data/model.pt").unwrap();
        assert_eq!(key.as_str(), "protocols/s3/global/data/model.pt");

        let key = s3_object_key("a".repeat(64).as_str(), "nested/path/x.txt").unwrap();
        assert_eq!(
            key.as_str(),
            format!("protocols/s3/{}/nested/path/x.txt", "a".repeat(64))
        );
    }

    #[test]
    fn object_key_constructors_share_key_validation() {
        for (key, error) in [
            ("", S3KeyError::EmptyKey),
            ("/leading", S3KeyError::LeadingSlash),
            ("/abs", S3KeyError::LeadingSlash),
            ("bad\nkey", S3KeyError::ControlCharacter),
            ("bad\u{7}", S3KeyError::ControlCharacter),
        ] {
            assert_eq!(S3ObjectKey::parse(key), Err(error), "{key:?}");
            assert_eq!(s3_object_key("global", key), Err(error), "{key:?}");
        }
        // Path safety belongs to the assembled storage key, not the client key.
        for key in ["../escape", "../evil", "a/../evil"] {
            assert_eq!(S3ObjectKey::parse(key).unwrap().as_str(), key);
            assert!(matches!(
                s3_object_key("global", key),
                Err(S3KeyError::ObjectKey(_))
            ));
        }
        for key in ["data/model.pt", "ünïcode/🦆.txt", "100%/done", "100_done"] {
            assert_eq!(S3ObjectKey::parse(key).unwrap().as_str(), key);
            assert_eq!(
                s3_object_key("global", key).unwrap().as_str(),
                format!("protocols/s3/global/{key}")
            );
        }
    }

    #[test]
    fn s3_object_key_rejects_oversized_assembled_path() {
        let result = s3_object_key("global", &"k".repeat(8192));
        assert!(matches!(result, Err(S3KeyError::ObjectKey(_))));
    }

    #[test]
    fn s3_object_key_length_limit_boundary() {
        // The assembled path may not exceed MAX_OBJECT_KEY_BYTES (4096).
        let prefix_len = "protocols/s3/global/".len();
        let at_limit = "k".repeat(4096 - prefix_len);
        assert!(s3_object_key("global", &at_limit).is_ok());
        let over_limit = "k".repeat(4096 - prefix_len + 1);
        assert!(matches!(
            s3_object_key("global", &over_limit),
            Err(S3KeyError::ObjectKey(_))
        ));
    }
}
