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
/// name `b.c`). Owners containing a `.` are not addressable and are rejected.
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
/// the decoded owner contains a `.`, the bucket is not lowercase or exceeds
/// 63 characters, or the owner or name is empty.
pub fn decode_bucket(bucket: &str) -> Result<(String, String), BucketDecodeError> {
    let Some((owner, name)) = bucket.split_once('.') else {
        return Err(BucketDecodeError::MissingSeparator);
    };
    if owner.contains('.') {
        return Err(BucketDecodeError::OwnerContainsDot);
    }
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
    if key.is_empty() {
        return Err(S3KeyError::EmptyKey);
    }
    if key.starts_with('/') {
        return Err(S3KeyError::LeadingSlash);
    }
    if key.chars().any(char::is_control) {
        return Err(S3KeyError::ControlCharacter);
    }
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
        assert_eq!(
            decode_bucket("acme.models").unwrap(),
            ("acme".to_owned(), "models".to_owned())
        );
        // `a.b.c` splits on the first `.`: owner `a`, name `b.c`.
        assert_eq!(
            decode_bucket("a.b.c").unwrap(),
            ("a".to_owned(), "b.c".to_owned())
        );
        // Round-trips for names containing dots.
        let encoded = encode_bucket("a", "b.c");
        assert_eq!(
            decode_bucket(&encoded).unwrap(),
            ("a".to_owned(), "b.c".to_owned())
        );
    }

    #[test]
    fn bucket_decode_rejects_missing_separator() {
        assert!(matches!(
            decode_bucket("acme"),
            Err(BucketDecodeError::MissingSeparator)
        ));
        assert!(matches!(
            decode_bucket(""),
            Err(BucketDecodeError::MissingSeparator)
        ));
    }

    #[test]
    fn bucket_decode_dotted_owner_semantics() {
        // The owner is the segment before the FIRST dot, so an owner can never
        // itself contain a dot: multi-dot buckets decode to a dotted NAME
        // (`a` / `b.c`) per the frozen `a.b.c` → (`a`, `b.c`) contract.
        assert_eq!(
            decode_bucket("a.b.c").unwrap(),
            ("a".to_owned(), "b.c".to_owned())
        );
        assert_eq!(
            decode_bucket("ac.me.models").unwrap(),
            ("ac".to_owned(), "me.models".to_owned())
        );
        // The `OwnerContainsDot` guard is a hard validation error for owners
        // containing '.' (unreachable through split-on-first-dot, but part of
        // the frozen contract); assert the variant maps to the documented
        // message so Lane 2 can translate it to `404 NoSuchBucket`.
        let err = BucketDecodeError::OwnerContainsDot;
        assert_eq!(err.to_string(), "bucket owner must not contain a '.'");
        let _ = err; // Copy variant stays usable.
    }

    #[test]
    fn bucket_decode_rejects_uppercase() {
        assert!(matches!(
            decode_bucket("Acme.models"),
            Err(BucketDecodeError::NotLowercase)
        ));
        assert!(matches!(
            decode_bucket("acme.Models"),
            Err(BucketDecodeError::NotLowercase)
        ));
    }

    #[test]
    fn bucket_decode_rejects_oversized() {
        let bucket = format!("{}.{}", "a".repeat(63), "b".repeat(64));
        assert!(matches!(
            decode_bucket(&bucket),
            Err(BucketDecodeError::TooLong)
        ));
        // 63 characters total (including the separator) is accepted.
        assert!(decode_bucket(&format!("{}.{}", "a".repeat(31), "b".repeat(31))).is_ok());
    }

    #[test]
    fn bucket_decode_rejects_empty_owner_or_name() {
        assert!(matches!(
            decode_bucket(".models"),
            Err(BucketDecodeError::EmptyPart)
        ));
        assert!(matches!(
            decode_bucket("acme."),
            Err(BucketDecodeError::EmptyPart)
        ));
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
    fn s3_object_key_rejects_invalid_keys() {
        assert!(matches!(
            s3_object_key("global", ""),
            Err(S3KeyError::EmptyKey)
        ));
        assert!(matches!(
            s3_object_key("global", "/leading"),
            Err(S3KeyError::LeadingSlash)
        ));
        assert!(matches!(
            s3_object_key("global", "bad\nkey"),
            Err(S3KeyError::ControlCharacter)
        ));
        // Path traversal is rejected by ObjectKey::parse on the assembled path.
        assert!(matches!(
            s3_object_key("global", "../escape"),
            Err(S3KeyError::ObjectKey(_))
        ));
    }

    #[test]
    fn s3_object_key_rejects_oversized_assembled_path() {
        let result = s3_object_key("global", &"k".repeat(8192));
        assert!(matches!(result, Err(S3KeyError::ObjectKey(_))));
    }
}
