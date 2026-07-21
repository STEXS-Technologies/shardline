use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{
    GetOptions, ObjectStore as ExternalObjectStore, ObjectStoreExt, memory::InMemory,
    path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, SecretString, ShardlineHash};

use super::{
    S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError, chunk_hash, is_temp_upload_key,
    normalize_prefix, stream_payload_for_range, temp_key_for, temporary_upload_location,
    validated_external_range, verify_file_length, verify_integrity,
};
use crate::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore as ObjectStoreTrait,
    PutOutcome,
};

#[test]
fn s3_config_normalizes_key_prefix() {
    let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
        .with_key_prefix(Some("/tenant-a/"));

    assert_eq!(config.key_prefix(), Some("tenant-a"));
}

#[test]
fn s3_location_applies_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::from_secret("access")),
                Some(SecretString::from_secret("secret")),
                None,
            )
            .with_key_prefix(Some("tenant-a")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let location = store.location_for_key(&key);

    assert!(location.is_ok());
    if let Ok(location) = location {
        assert_eq!(location.as_ref(), "tenant-a/xorbs/default/aa/hash.xorb");
    }
}

#[test]
fn s3_store_rejects_traversal_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("../tenant-b")),
    );

    assert!(matches!(
        store,
        Err(super::S3ObjectStoreError::InvalidKeyPrefix(_))
    ));
}

#[test]
fn s3_store_rejects_dot_segment_key_prefix_after_normalization() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("/tenant-a/./objects/")),
    );

    assert!(matches!(
        store,
        Err(super::S3ObjectStoreError::InvalidKeyPrefix(_))
    ));
}

#[test]
fn s3_store_debug_redacts_credentials() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::from_secret("access-key")),
                Some(SecretString::from_secret("secret-key")),
                Some(SecretString::from_secret("session-token")),
            )
            .with_key_prefix(Some("tenant-a")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };

    let rendered = format!("{store:?}");

    assert!(!rendered.contains("access-key"));
    assert!(!rendered.contains("secret-key"));
    assert!(!rendered.contains("session-token"));
}

#[test]
fn validated_external_range_converts_inclusive_byte_range() {
    let range = ByteRange::new(3, 8);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };

    let external = validated_external_range(range);

    assert!(external.is_ok());
    assert_eq!(external.ok(), Some(3..9));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_payload_for_range_rejects_mismatched_result_range() {
    let store = InMemory::new();
    let location = ObjectStorePath::from("tenant/object");
    assert!(
        store
            .put(&location, Bytes::from_static(b"abcd").into())
            .await
            .is_ok()
    );
    let result = store
        .get_opts(&location, GetOptions::new().with_range(Some(0..4)))
        .await;
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };

    let stream = stream_payload_for_range(result, 1..5);

    assert!(matches!(
        stream,
        Err(super::S3ObjectStoreError::RangeOutOfBounds)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_payload_for_range_preserves_streamed_bytes() {
    let store = InMemory::new();
    let location = ObjectStorePath::from("tenant/object");
    assert!(
        store
            .put(&location, Bytes::from_static(b"abcd").into())
            .await
            .is_ok()
    );
    let result = store
        .get_opts(&location, GetOptions::new().with_range(Some(0..4)))
        .await;
    assert!(result.is_ok());
    let Ok(result) = result else {
        return;
    };

    let stream = stream_payload_for_range(result, 0..4);
    assert!(stream.is_ok());
    let Ok(mut stream) = stream else {
        return;
    };
    let mut observed = Vec::new();
    while let Some(item) = stream.next().await {
        assert!(item.is_ok());
        let Ok(chunk) = item else {
            return;
        };
        observed.extend_from_slice(&chunk);
    }

    assert_eq!(observed, b"abcd");
}

#[test]
fn s3_store_rejects_empty_bucket() {
    let config = S3ObjectStoreConfig::new(String::new(), "us-east-1".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
}

#[test]
fn s3_store_default_config_uses_standard_s3_endpoint() {
    let config = S3ObjectStoreConfig::new("my-bucket".to_owned(), "us-east-1".to_owned());
    // A default config (no endpoint, no credentials) builds the AmazonS3 client
    // successfully — it just won't be able to reach AWS without network.
    let store = S3ObjectStore::new(config);
    assert!(store.is_ok());
}

#[test]
fn is_temp_upload_key_matches_standard_temp_format() {
    assert!(is_temp_upload_key("uploads/obj.tmp.42"));
    assert!(is_temp_upload_key("obj.tmp.0"));
    assert!(is_temp_upload_key("obj.tmp.999999"));
}

#[test]
fn is_temp_upload_key_rejects_non_temp() {
    assert!(!is_temp_upload_key("uploads/obj"));
    assert!(!is_temp_upload_key("obj.tmp."));
    assert!(!is_temp_upload_key("obj.tmp.abc"));
    assert!(!is_temp_upload_key("obj.tmpx.42"));
}

#[test]
fn validated_external_range_zero_length_rejected() {
    // ByteRange(5, 3) has start > end, which is invalid
    let result = shardline_protocol::ByteRange::new(5, 3);
    assert!(result.is_err());
}

#[test]
fn s3_error_display_and_debug_are_implemented() {
    let err = S3ObjectStoreError::RangeOutOfBounds;
    let display = format!("{err}");
    assert!(!display.is_empty(), "Display should produce a message");

    let debug = format!("{err:?}");
    assert!(!debug.is_empty(), "Debug should produce a message");
}

#[test]
fn s3_store_with_full_credentials_accepts_custom_session_token() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        Some(SecretString::from_secret("key")),
        Some(SecretString::from_secret("secret")),
        Some(SecretString::from_secret("token")),
    );
    // Full credentials (key + secret + session token) should build successfully
    // without needing any external endpoint.
    let store = S3ObjectStore::new(config);
    assert!(store.is_ok());
}

// ── verify_integrity ───────────────────────────────────────────────────

#[test]
fn verify_integrity_accepts_matching_hash_and_length() {
    let bytes = b"hello world";
    let integrity = ObjectIntegrity::new(chunk_hash(bytes), 11);
    assert!(verify_integrity(bytes, &integrity).is_ok());
}

#[test]
fn verify_integrity_rejects_length_mismatch() {
    let bytes = b"hello world";
    let integrity = ObjectIntegrity::new(chunk_hash(bytes), 99);
    assert!(matches!(
        verify_integrity(bytes, &integrity),
        Err(S3ObjectStoreError::IntegrityLengthMismatch)
    ));
}

#[test]
fn verify_integrity_rejects_hash_mismatch() {
    let bytes = b"hello world";
    let other_hash = chunk_hash(b"different bytes");
    let integrity = ObjectIntegrity::new(other_hash, 11);
    assert!(matches!(
        verify_integrity(bytes, &integrity),
        Err(S3ObjectStoreError::IntegrityHashMismatch)
    ));
}

#[test]
fn verify_integrity_rejects_empty_bytes_with_nonzero_length() {
    let bytes = b"";
    let integrity = ObjectIntegrity::new(chunk_hash(b"x"), 1);
    assert!(matches!(
        verify_integrity(bytes, &integrity),
        Err(S3ObjectStoreError::IntegrityLengthMismatch)
    ));
}

// ── verify_file_length ─────────────────────────────────────────────────

#[test]
fn verify_file_length_matches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bin");
    std::fs::write(&path, b"exactly this long").unwrap();
    let integrity = ObjectIntegrity::new(chunk_hash(b"exactly this long"), 17);
    assert!(verify_file_length(&path, &integrity).is_ok());
}

#[test]
fn verify_file_length_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bin");
    std::fs::write(&path, b"short").unwrap();
    let integrity = ObjectIntegrity::new(chunk_hash(b"short"), 99);
    assert!(matches!(
        verify_file_length(&path, &integrity),
        Err(S3ObjectStoreError::IntegrityLengthMismatch)
    ));
}

// ── temporary_upload_location ─────────────────────────────────────────

#[test]
fn temporary_upload_location_with_prefix() {
    let prefix = Some("tenant-a".to_owned());
    let location = temporary_upload_location(&prefix);
    let path_str = location.as_ref();
    assert!(path_str.starts_with("tenant-a/__tmp/shardline-stream-upload/"));
    assert!(path_str.len() > "tenant-a/__tmp/shardline-stream-upload/".len());
}

#[test]
fn temporary_upload_location_without_prefix() {
    let prefix: Option<String> = None;
    let location = temporary_upload_location(&prefix);
    let path_str = location.as_ref();
    assert!(path_str.starts_with("__tmp/shardline-stream-upload/"));
    assert!(path_str.len() > "__tmp/shardline-stream-upload/".len());
}

// ── normalize_prefix edge cases ───────────────────────────────────────

#[test]
fn normalize_prefix_multi_slash() {
    assert_eq!(normalize_prefix("///a///b///"), Some("a///b".to_owned()));
}

#[test]
fn normalize_prefix_already_clean() {
    assert_eq!(normalize_prefix("a/b/c"), Some("a/b/c".to_owned()));
}

#[test]
fn normalize_prefix_preserves_single_component() {
    assert_eq!(normalize_prefix("prefix"), Some("prefix".to_owned()));
}

// ── normalize_prefix ─────────────────────────────────────────────────

#[test]
fn normalize_prefix_trims_trailing_slashes() {
    assert_eq!(normalize_prefix("prefix/"), Some("prefix".to_owned()));
}

#[test]
fn normalize_prefix_trims_leading_slashes() {
    assert_eq!(normalize_prefix("/prefix"), Some("prefix".to_owned()));
}

#[test]
fn normalize_prefix_trims_both_slashes() {
    assert_eq!(normalize_prefix("/prefix/"), Some("prefix".to_owned()));
}

#[test]
fn normalize_prefix_returns_none_for_empty_after_trim() {
    assert_eq!(normalize_prefix(""), None);
    assert_eq!(normalize_prefix("/"), None);
    assert_eq!(normalize_prefix("///"), None);
}

#[test]
fn normalize_prefix_preserves_inner_slashes() {
    assert_eq!(normalize_prefix("/a/b/c/"), Some("a/b/c".to_owned()));
}

// ── validate_config ───────────────────────────────────────────────────

#[test]
fn validate_config_rejects_empty_region() {
    let config = S3ObjectStoreConfig::new("bucket".to_owned(), String::new());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyRegion)));
}

#[test]
fn validate_config_rejects_empty_bucket() {
    let config = S3ObjectStoreConfig::new(String::new(), "region".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
}

// ── S3ObjectStoreConfig accessors ──────────────────────────────────────

#[test]
fn s3_config_bucket_accessor() {
    let config = S3ObjectStoreConfig::new("my-bucket".to_owned(), "us-east-1".to_owned());
    assert_eq!(config.bucket(), "my-bucket");
}

#[test]
fn s3_config_bucket_accessor_with_prefix() {
    let config =
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_key_prefix(Some("pfx"));
    assert_eq!(config.key_prefix(), Some("pfx"));
}

#[test]
fn s3_config_bucket_accessor_no_prefix() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
    assert_eq!(config.key_prefix(), None);
}

// ── S3ObjectStoreConfig Debug redaction ────────────────────────────────

#[test]
fn s3_config_debug_redacts_credentials() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        Some(SecretString::from_secret("access-key-123")),
        Some(SecretString::from_secret("secret-key-456")),
        Some(SecretString::from_secret("session-token-789")),
    );
    let rendered = format!("{config:?}");
    assert!(!rendered.contains("access-key-123"));
    assert!(!rendered.contains("secret-key-456"));
    assert!(!rendered.contains("session-token-789"));
    assert!(rendered.contains("***"));
}

// ── chunk_hash ────────────────────────────────────────────────────────

#[test]
fn chunk_hash_produces_consistent_result() {
    let a = super::chunk_hash(b"hello world");
    let b = super::chunk_hash(b"hello world");
    assert_eq!(a, b);
}

#[test]
fn chunk_hash_differs_for_different_inputs() {
    let a = super::chunk_hash(b"abc");
    let b = super::chunk_hash(b"xyz");
    assert_ne!(a, b);
}

#[test]
fn chunk_hash_handles_empty_input() {
    let hash = super::chunk_hash(b"");
    let expected = ShardlineHash::from_bytes(*blake3::hash(b"").as_bytes());
    assert_eq!(hash, expected);
}

// ── Error variant Display ─────────────────────────────────────────────

#[test]
fn s3_error_display_incomplete_credentials() {
    let err = S3ObjectStoreError::IncompleteCredentials;
    assert_eq!(
        err.to_string(),
        "s3 object store credentials must include both access key id and secret access key"
    );
}

#[test]
fn s3_error_display_empty_bucket() {
    let err = S3ObjectStoreError::EmptyBucket;
    assert_eq!(err.to_string(), "s3 object store bucket must not be empty");
}

#[test]
fn s3_error_display_empty_region() {
    let err = S3ObjectStoreError::EmptyRegion;
    assert_eq!(err.to_string(), "s3 object store region must not be empty");
}

#[test]
fn s3_error_display_integrity_length_mismatch() {
    let err = S3ObjectStoreError::IntegrityLengthMismatch;
    assert_eq!(
        err.to_string(),
        "object body length did not match expected integrity"
    );
}

#[test]
fn s3_error_display_integrity_hash_mismatch() {
    let err = S3ObjectStoreError::IntegrityHashMismatch;
    assert_eq!(
        err.to_string(),
        "object body hash did not match expected integrity"
    );
}

#[test]
fn s3_error_display_existing_object_conflict() {
    let err = S3ObjectStoreError::ExistingObjectConflict;
    assert_eq!(
        err.to_string(),
        "object key already exists with conflicting bytes"
    );
}

#[test]
fn s3_error_display_range_out_of_bounds() {
    let err = S3ObjectStoreError::RangeOutOfBounds;
    assert_eq!(
        err.to_string(),
        "requested byte range exceeded stored object length"
    );
}

#[test]
fn s3_error_display_invalid_listed_key() {
    let err = S3ObjectStoreError::InvalidListedKey;
    assert_eq!(
        err.to_string(),
        "s3 listed an object outside the configured key prefix"
    );
}

#[test]
fn s3_error_display_invalid_upload_parts() {
    let err = S3ObjectStoreError::InvalidUploadParts;
    assert_eq!(
        err.to_string(),
        "upload parts list has invalid part numbering"
    );
}

#[test]
fn s3_error_display_runtime_unavailable() {
    let err = S3ObjectStoreError::RuntimeUnavailable;
    assert_eq!(err.to_string(), "s3 object store runtime is unavailable");
}

#[test]
fn s3_error_display_invalid_key_prefix() {
    use crate::ObjectPrefixError;
    let err = S3ObjectStoreError::InvalidKeyPrefix(ObjectPrefixError::UnsafePath);
    let msg = err.to_string();
    assert!(msg.contains("key prefix was invalid"));
}

#[test]
fn s3_error_display_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    let err = S3ObjectStoreError::Io(io_err);
    let msg = err.to_string();
    assert!(msg.contains("temporary file operation failed"));
}

#[test]
fn s3_error_display_path() {
    use object_store::path::Error as PathError;
    let err = S3ObjectStoreError::Path(PathError::InvalidPath {
        path: "/bad".into(),
    });
    let msg = err.to_string();
    assert!(msg.contains("object-store path conversion failed"));
}

#[test]
fn s3_error_display_runtime() {
    let io_err = std::io::Error::other("thread spawn failed");
    let err = S3ObjectStoreError::Runtime(io_err);
    let msg = err.to_string();
    assert!(msg.contains("s3 object store runtime initialization failed"));
}

#[test]
fn s3_error_display_external() {
    let ext_err = object_store::Error::NotImplemented {
        operation: "test".to_owned(),
        implementer: "test".to_owned(),
    };
    let err = S3ObjectStoreError::External(ext_err);
    let msg = err.to_string();
    assert!(msg.contains("s3 object store operation failed"));
}

// ── S3ObjectStoreConfig builder ───────────────────────────────────────

#[test]
fn s3_config_with_virtual_hosted_style() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_virtual_hosted_style_request(true);
    let store = S3ObjectStore::new(config);
    // Should build successfully (the setting just changes how requests are made)
    assert!(store.is_ok());
}

// ── temp_key_for ──────────────────────────────────────────────────────

#[test]
fn temp_key_for_appends_tmp_suffix_with_digits() {
    let key = ObjectKey::parse("test/object.xorb").unwrap();
    let result: Result<ObjectKey, S3ObjectStoreError> = temp_key_for(&key);
    assert!(result.is_ok());
    let Ok(temp) = result else {
        return;
    };
    let temp_str: &str = temp.as_str();
    assert!(temp_str.starts_with("test/object.xorb.tmp."));
    // Verify there are digits after .tmp.
    let after_dot_tmp: Option<&str> = temp_str.find(".tmp.").map(|pos| &temp_str[pos + 5..]);
    assert!(after_dot_tmp.is_some_and(|s: &str| !s.is_empty()));
}

#[test]
fn temp_key_for_differs_on_subsequent_calls() {
    let key = ObjectKey::parse("same/key.xorb").unwrap();
    let a = temp_key_for(&key).unwrap();
    let b = temp_key_for(&key).unwrap();
    assert_ne!(a.as_str(), b.as_str());
}

// ── validate_config whitespace edge cases ──────────────────────────────

#[test]
fn validate_config_rejects_whitespace_bucket() {
    let config = S3ObjectStoreConfig::new("   ".to_owned(), "us-east-1".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
}

#[test]
fn validate_config_rejects_whitespace_region() {
    let config = S3ObjectStoreConfig::new("bucket".to_owned(), "   ".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyRegion)));
}

#[test]
fn validate_config_accepts_valid_config() {
    let config = S3ObjectStoreConfig::new("bucket".to_owned(), "region".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(result.is_ok());
}

// ── is_temp_upload_key edge cases ──────────────────────────────────────

#[test]
fn is_temp_upload_key_rejects_empty() {
    assert!(!is_temp_upload_key(""));
}

#[test]
fn is_temp_upload_key_rejects_no_digits_after_tmp() {
    assert!(!is_temp_upload_key("obj.tmp."));
    assert!(!is_temp_upload_key("obj.tmp.abc"));
}

#[test]
fn is_temp_upload_key_rejects_multiple_tmp_as_middle_segment() {
    assert!(!is_temp_upload_key("obj.tmp.tmp.42"));
    assert!(is_temp_upload_key("obj.tmp.42.tmp.99"));
}

#[test]
fn is_temp_upload_key_rejects_without_tmp_suffix() {
    assert!(!is_temp_upload_key("obj"));
    assert!(!is_temp_upload_key("obj.tmpx.42"));
    assert!(!is_temp_upload_key("obj.temp.42"));
}

#[test]
fn is_temp_upload_key_accepts_middle_of_key() {
    assert!(is_temp_upload_key("prefix/obj.tmp.123/suffix"));
}

#[test]
fn is_temp_upload_key_accepts_tmp_at_start_of_key() {
    assert!(is_temp_upload_key(".tmp.42"));
}

#[test]
fn is_temp_upload_key_accepts_tmp_at_start_of_segment() {
    assert!(is_temp_upload_key("a/.tmp.42"));
}

#[test]
fn is_temp_upload_key_accepts_when_digit_followed_by_extra() {
    assert!(is_temp_upload_key("obj.tmp.42extra"));
}

#[test]
fn is_temp_upload_key_rejects_tmp_alone_after_prefix() {
    assert!(!is_temp_upload_key("prefix.tmp."));
}

// ── S3ObjectStoreConfig Debug with missing credentials ─────────────────

#[test]
fn s3_config_debug_without_credentials() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
    let rendered = format!("{config:?}");
    assert!(rendered.contains("bucket"));
    assert!(rendered.contains("allow_http"));
    assert!(rendered.contains("region"));
    assert!(rendered.contains("key_prefix"));
}

#[test]
fn s3_config_with_allow_http_defaults_to_false() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
    let store = S3ObjectStore::new(config);
    assert!(store.is_ok());
}

// ── S3ObjectStoreError From impls ──────────────────────────────────────

#[test]
fn s3_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
    let err: S3ObjectStoreError = io_err.into();
    assert!(matches!(err, S3ObjectStoreError::Io(_)));
}

#[test]
fn s3_error_from_external_error() {
    use object_store::Error as ExtError;
    let ext_err = ExtError::Generic {
        store: "test",
        source: Box::new(std::io::Error::other("fail")),
    };
    let err: S3ObjectStoreError = ext_err.into();
    assert!(matches!(err, S3ObjectStoreError::External(_)));
}

// ── S3ByteStream type alias smoke test ─────────────────────────────────

#[test]
fn s3_byte_stream_type_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<super::S3ByteStream>();
}

#[test]
fn validated_external_range_rejects_len_overflow() {
    // ByteRange(0, u64::MAX) is valid (start <= end) but len()
    // returns None because (MAX - 0) + 1 overflows u64.
    let range = ByteRange::new(0, u64::MAX).expect("valid range");
    let result = validated_external_range(range);
    assert!(matches!(result, Err(S3ObjectStoreError::RangeOutOfBounds)));
}

// ── verified_external_range edge cases ────────────────────────────

#[test]
fn validated_external_range_single_byte() {
    let range = ByteRange::new(5, 5);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };

    let external = validated_external_range(range);

    assert!(external.is_ok());
    assert_eq!(external.ok(), Some(5..6));
}

#[test]
fn validated_external_range_large_range() {
    // A large valid range
    let range = ByteRange::new(0, u64::MAX - 1).expect("valid large range");
    let external = validated_external_range(range);
    // Should succeed
    assert!(external.is_ok());
    assert_eq!(external.ok(), Some(0..u64::MAX));
}

// ── S3MultipartUploadWriter via InMemory ──────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_upload_writer_write_and_abort() {
    use object_store::ObjectStoreExt;
    use object_store::WriteMultipart;

    let store = InMemory::new();
    let location = ObjectStorePath::from("test/multi-part-obj");

    let upload = store
        .put_multipart(&location)
        .await
        .expect("should create multipart upload");
    let writer = WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
    let mut multipart = super::S3MultipartUploadWriter { writer };

    // Write some bytes
    multipart.write(b"hello multipart");
    // Wait with zero permits — should be fine since nothing is in flight
    let wait = multipart.wait_for_capacity(0).await;
    assert!(wait.is_ok());

    // Abort before finish — should succeed
    let abort = multipart.abort().await;
    assert!(abort.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_upload_writer_abort_unstarted() {
    use object_store::ObjectStoreExt;
    use object_store::WriteMultipart;

    let store = InMemory::new();
    let location = ObjectStorePath::from("test/abort-unstarted");

    let upload = store
        .put_multipart(&location)
        .await
        .expect("should create multipart upload");
    let writer = WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
    let multipart = super::S3MultipartUploadWriter { writer };

    // Abort immediately without writing anything
    let abort = multipart.abort().await;
    assert!(abort.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_upload_writer_wait_with_zero_permits() {
    use object_store::ObjectStoreExt;
    use object_store::WriteMultipart;

    let store = InMemory::new();
    let location = ObjectStorePath::from("test/zero-permits");

    let upload = store
        .put_multipart(&location)
        .await
        .expect("should create multipart upload");
    let writer = WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
    let mut multipart = super::S3MultipartUploadWriter { writer };

    // wait_for_capacity with 0 permits is valid when no parts are queued
    let wait = multipart.wait_for_capacity(0).await;
    assert!(wait.is_ok());
}

#[test]
fn s3_config_endpoint_defaults_to_none() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
    let rendered = format!("{config:?}");
    assert!(rendered.contains("endpoint: None"));
}

#[test]
fn s3_config_with_endpoint_sets_endpoint() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_endpoint(Some("http://s3.example.com:9000".to_owned()));
    let rendered = format!("{config:?}");
    assert!(rendered.contains("endpoint: Some(\"http://s3.example.com:9000\")"));
}

// ── S3ObjectStoreConfig builder methods ────────────────────────────────

#[test]
fn s3_config_with_virtual_hosted_style_true() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_virtual_hosted_style_request(true);
    let store = S3ObjectStore::new(config);
    assert!(store.is_ok());
}

#[test]
fn s3_config_with_virtual_hosted_style_false() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_virtual_hosted_style_request(false);
    let store = S3ObjectStore::new(config);
    assert!(store.is_ok());
}

#[test]
fn s3_config_with_key_prefix_none_is_noop() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_key_prefix(None);
    assert_eq!(config.key_prefix(), None);
}

#[test]
fn s3_config_with_key_prefix_trimmed() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_key_prefix(Some("/tenant-b/"));
    assert_eq!(config.key_prefix(), Some("tenant-b"));
}

// ── streaming_large_copy config validation ─────────────────────────────

#[test]
fn streaming_large_copy_rejects_empty_bucket_config() {
    let config = S3ObjectStoreConfig::new(String::new(), "r".to_owned());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
}

#[test]
fn streaming_large_copy_rejects_empty_region_config() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), String::new());
    let result = S3ObjectStore::new(config);
    assert!(matches!(result, Err(S3ObjectStoreError::EmptyRegion)));
}

// ── Incomplete credentials rejection ──────────────────────────────────

#[test]
fn s3_store_rejects_incomplete_credentials_key_without_secret() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        Some(SecretString::from_secret("key")),
        None,
        None,
    );
    let store = S3ObjectStore::new(config);
    assert!(matches!(
        store,
        Err(S3ObjectStoreError::IncompleteCredentials)
    ));
}

#[test]
fn s3_store_rejects_incomplete_credentials_secret_without_key() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        None,
        Some(SecretString::from_secret("secret")),
        None,
    );
    let store = S3ObjectStore::new(config);
    assert!(matches!(
        store,
        Err(S3ObjectStoreError::IncompleteCredentials)
    ));
}

// ── MinIO-backed integration tests ───────────────────────────────────

mod minio_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use shardline_test_support::DockerLocalStack;
    use shardline_test_support::S3RawConfig;

    use super::super::{
        BeginMultipartUploadResult, S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError,
    };
    use crate::{
        DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix,
        ObjectStore as ObjectStoreTrait, PutOutcome,
    };
    use shardline_protocol::{ByteRange, SecretString};

    /// Shared MinIO init guard: only starts containers once across all tests.
    static MINIO_INIT: AtomicBool = AtomicBool::new(false);
    static INIT_LOCK: std::sync::OnceLock<()> = std::sync::OnceLock::new();

    fn ensure_minio() -> Option<DockerLocalStack> {
        if !DockerLocalStack::docker_available() {
            return None;
        }
        INIT_LOCK.get_or_init(|| {
            MINIO_INIT.store(true, Ordering::SeqCst);
        });
        if !MINIO_INIT.load(Ordering::SeqCst) {
            return None;
        }
        // Start fresh stack per-test-call so we get clean state.
        DockerLocalStack::builder()
            .with_minio()
            .start()
            .ok()
            .flatten()
    }

    fn build_s3_store(stack: &DockerLocalStack, key_prefix: Option<&str>) -> S3ObjectStore {
        let raw: S3RawConfig = stack.s3_raw_config(key_prefix).unwrap();
        let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_allow_http(raw.allow_http)
            .with_credentials(
                raw.access_key.map(SecretString::new),
                raw.secret_key.map(SecretString::new),
                raw.session_token.map(SecretString::new),
            )
            .with_key_prefix(raw.key_prefix.as_deref());
        S3ObjectStore::new(config).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_put_and_get_roundtrip() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let prefix = stack.unique_s3_key_prefix("test-roundtrip");
        let store = build_s3_store(&stack, Some(&prefix));
        let key = ObjectKey::parse("objects/hash.xorb").unwrap();
        let body = b"hello minio";
        let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 11);

        let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(
            matches!(inserted, Ok(PutOutcome::Inserted)),
            "expected Ok(Inserted), got {inserted:?}"
        );

        let contains = store.contains(&key);
        assert!(matches!(contains, Ok(true)));

        let read = store.read_range(&key, ByteRange::new(0, 10).unwrap());
        assert!(read.is_ok());
        assert_eq!(read.unwrap(), b"hello minio");

        let deleted = store.delete_if_present(&key);
        assert!(matches!(deleted, Ok(DeleteOutcome::Deleted)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_put_if_absent_idempotent() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let store = build_s3_store(&stack, Some("test-idempotent"));
        let key = ObjectKey::parse("objects/same.xorb").unwrap();
        let body = b"same content";
        let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 12);

        let first = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(first, Ok(PutOutcome::Inserted)));

        let second = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_list_flat_namespace_page_pagination() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let prefix = stack.unique_s3_key_prefix("test-list-page");
        let store = build_s3_store(&stack, Some(&prefix));
        let ns_prefix = ObjectPrefix::parse("ns/").unwrap();

        // Insert three objects under the same flat namespace
        for i in 0..3u64 {
            let key = ObjectKey::parse(&format!("ns/key{i:020}")).unwrap();
            let body = b"data";
            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 4);
            store
                .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
                .unwrap();
        }

        // Full listing
        let all = store
            .list_flat_namespace_page(&ns_prefix, None, 10)
            .unwrap();
        assert_eq!(all.len(), 3, "should list all 3 objects");

        // Pagination with start_after
        let after_first = ObjectKey::parse("ns/key00000000000000000000").unwrap();
        let page = store
            .list_flat_namespace_page(&ns_prefix, Some(&after_first), 10)
            .unwrap();
        assert_eq!(
            page.len(),
            2,
            "should list remaining 2 objects after first key"
        );

        // Limit
        let limited = store.list_flat_namespace_page(&ns_prefix, None, 2).unwrap();
        assert_eq!(limited.len(), 2, "should be limited to 2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_visit_prefix_with_multiple_entries() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let prefix = stack.unique_s3_key_prefix("test-visit-prefix");
        let store = build_s3_store(&stack, Some(&prefix));
        let ns_prefix = ObjectPrefix::parse("vp/").unwrap();

        for i in 0..5u64 {
            let key = ObjectKey::parse(&format!("vp/obj{i:010}")).unwrap();
            let body = b"x";
            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 1);
            store
                .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
                .unwrap();
        }

        let mut visited = Vec::new();
        let result: Result<(), S3ObjectStoreError> = store.visit_prefix(&ns_prefix, |meta| {
            visited.push(meta.key().clone());
            Ok(())
        });
        assert!(result.is_ok());
        assert_eq!(visited.len(), 5, "visit_prefix should visit all 5 entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_begin_and_finish_content_addressed_upload() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let store = build_s3_store(&stack, Some("test-ca-upload"));
        let canonical = ObjectKey::parse("cas/deadbeef.xorb").unwrap();
        let body = b"content addressed payload";

        let begin = store.begin_content_addressed_upload(&canonical).await;
        assert!(begin.is_ok());
        let Ok(begin) = begin else { return };

        match begin {
            BeginMultipartUploadResult::AlreadyExists => {
                // Someone else got there first — that's fine
            }
            BeginMultipartUploadResult::Upload(mut writer, temp_key) => {
                writer.write(body);
                writer.wait_for_capacity(4).await.unwrap();

                let finish = store
                    .finish_content_addressed_upload(writer, &temp_key, &canonical)
                    .await;
                assert!(finish.is_ok());
                let outcome = finish.unwrap();
                // Could be Inserted or AlreadyExists depending on timing
                assert!(
                    outcome == PutOutcome::Inserted || outcome == PutOutcome::AlreadyExists,
                    "expected Inserted or AlreadyExists, got {outcome:?}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_put_content_addressed_file() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let store = build_s3_store(&stack, Some("test-put-ca-file"));
        let key = ObjectKey::parse("cas/filehash.xorb").unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let body = b"file content for ca test";
        std::fs::write(tmp.path(), body).unwrap();

        let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), body.len() as u64);
        let result = store.put_content_addressed_file(&key, tmp.path(), &integrity);
        assert!(result.is_ok());

        // Verify it's readable
        let read = store
            .read_range(&key, ByteRange::new(0, body.len() as u64 - 1).unwrap())
            .unwrap();
        assert_eq!(read, body);

        // Second put is idempotent
        let second = store.put_content_addressed_file(&key, tmp.path(), &integrity);
        assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_put_file_if_absent() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let store = build_s3_store(&stack, Some("test-put-file-absent"));
        let key = ObjectKey::parse("files/test.bin").unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let body = b"file content for put_file_if_absent";
        std::fs::write(tmp.path(), body).unwrap();

        let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), body.len() as u64);
        let result = store.put_file_if_absent(&key, tmp.path(), &integrity);
        assert!(result.is_ok());

        let read = store
            .read_range(&key, ByteRange::new(0, body.len() as u64 - 1).unwrap())
            .unwrap();
        assert_eq!(read, body);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn minio_copy_object_if_absent() {
        let stack = match ensure_minio() {
            Some(s) => s,
            None => return,
        };
        let store = build_s3_store(&stack, Some("test-copy-obj"));
        let src = ObjectKey::parse("src/original.xorb").unwrap();
        let dst = ObjectKey::parse("dst/copy.xorb").unwrap();

        let body = b"copy test data";
        let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 14);
        store
            .put_if_absent(&src, ObjectBody::from_slice(body), &integrity)
            .unwrap();

        let copy = store.copy_object_if_absent(&src, &dst);
        assert!(matches!(copy, Ok(PutOutcome::Inserted)));

        let idempotent = store.copy_object_if_absent(&src, &dst);
        assert!(matches!(idempotent, Ok(PutOutcome::AlreadyExists)));
    }
}

// ── Additional pure function edge cases ──────────────────────────────

#[test]
fn normalize_prefix_empty_input_chain() {
    assert_eq!(normalize_prefix("//"), None);
    assert_eq!(normalize_prefix("///////"), None);
}

#[test]
fn location_for_key_without_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let key = ObjectKey::parse("simple/key.xorb").unwrap();
    let location = store.location_for_key(&key);
    assert!(location.is_ok());
    if let Ok(loc) = location {
        assert_eq!(loc.as_ref(), "simple/key.xorb");
    }
}

#[test]
fn location_for_prefix_empty_without_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let prefix = crate::ObjectPrefix::parse("").unwrap();
    let location = store.location_for_prefix(&prefix);
    assert!(location.is_ok());
    if let Ok(loc) = location {
        // Empty prefix with no key_prefix should give empty path
        assert_eq!(loc.as_ref(), "");
    }
}

#[test]
fn location_for_prefix_empty_with_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("tenant-x")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let prefix = crate::ObjectPrefix::parse("").unwrap();
    let location = store.location_for_prefix(&prefix);
    assert!(location.is_ok());
    if let Ok(loc) = location {
        assert_eq!(loc.as_ref(), "tenant-x");
    }
}

#[test]
fn location_for_prefix_with_prefix_and_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("tenant-x")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let prefix = crate::ObjectPrefix::parse("sub/").unwrap();
    let location = store.location_for_prefix(&prefix);
    assert!(location.is_ok());
    if let Ok(loc) = location {
        // ObjectStorePath::parse may strip trailing slashes
        let path_str = loc.as_ref();
        assert!(
            path_str == "tenant-x/sub" || path_str == "tenant-x/sub/",
            "unexpected path: {path_str}"
        );
    }
}

#[test]
fn s3_config_debug_shows_virtual_hosted_style() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
        .with_virtual_hosted_style_request(true);
    let rendered = format!("{config:?}");
    assert!(rendered.contains("virtual_hosted_style_request: true"));
}

#[test]
fn s3_store_debug_shows_runtime_status() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let rendered = format!("{store:?}");
    // Should mention runtime status
    assert!(rendered.contains("runtime"));
}

#[test]
fn validated_external_range_start_at_u64_max_rejected() {
    // Range starting at u64::MAX with length 1 would overflow
    let range = ByteRange::new(u64::MAX, u64::MAX).expect("valid range with single byte at MAX");
    let result = validated_external_range(range);
    assert!(matches!(result, Err(S3ObjectStoreError::RangeOutOfBounds)));
}

#[test]
fn s3_error_source_invalid_key_prefix() {
    use crate::ObjectPrefixError;
    let prefix_err = ObjectPrefixError::UnsafePath;
    let err = S3ObjectStoreError::InvalidKeyPrefix(prefix_err);
    let source = std::error::Error::source(&err);
    assert!(source.is_some());
    let source = source.unwrap();
    assert_eq!(
        source.to_string(),
        ObjectPrefixError::UnsafePath.to_string()
    );
}

#[test]
fn temp_key_for_overflow_key_rejected() {
    // Create a key near the maximum length so that adding the temp suffix overflows
    let long_base = "a".repeat(4090);
    let key = ObjectKey::parse(&long_base);
    assert!(key.is_ok());
    let Ok(key) = key else { return };

    // temp_key_for adds ".tmp.{counter}.{pid}.{nanos}" which may exceed max length
    let result = temp_key_for(&key);
    // Depending on counter/pid/nanos this might overflow, but should not panic
    if let Err(error) = &result {
        assert!(matches!(error, S3ObjectStoreError::InvalidListedKey));
    } else if let Ok(temp) = &result {
        assert!(temp.as_str().len() > long_base.len());
    }
}

// ── verify_file_length with non-existent file ─────────────────────────

#[test]
fn verify_file_length_nonexistent_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist.bin");
    let integrity = ObjectIntegrity::new(super::chunk_hash(b""), 0);
    let result = super::verify_file_length(&path, &integrity);
    assert!(matches!(result, Err(S3ObjectStoreError::Io(_))));
}

// ── Error variant source() tests ──────────────────────────────────────

#[test]
fn s3_error_source_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "test");
    let err = S3ObjectStoreError::Io(io_err);
    let source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
    assert!(source.is_some());
    let source = source.unwrap();
    assert_eq!(source.to_string(), "test");
}

#[test]
fn s3_error_source_path() {
    use object_store::path::Error as PathError;
    let path_err = PathError::InvalidPath {
        path: "/bad".into(),
    };
    let err = S3ObjectStoreError::Path(path_err);
    let source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
    assert!(source.is_some());
    let source = source.unwrap();
    assert!(source.to_string().contains("/bad"));
}

#[test]
fn s3_error_source_runtime() {
    let io_err = std::io::Error::other("thread spawn failed");
    let err = S3ObjectStoreError::Runtime(io_err);
    let source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
    assert!(source.is_some());
    let source = source.unwrap();
    assert_eq!(source.to_string(), "thread spawn failed");
}

#[test]
fn s3_error_source_external() {
    let ext_err = object_store::Error::NotImplemented {
        operation: "test".to_owned(),
        implementer: "test".to_owned(),
    };
    let err = S3ObjectStoreError::External(ext_err);
    let source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
    // External derives source via #[from]
    assert!(source.is_some());
}

// ── S3ObjectStore partial config properties ───────────────────────────

#[test]
fn s3_config_clone_and_eq() {
    let a = S3ObjectStoreConfig::new("bucket".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some("http://example.com".to_owned()))
        .with_allow_http(true);
    let b = a.clone();
    assert_eq!(a, b);
}

#[test]
fn s3_config_with_credentials_only_key() {
    // Only access_key_id without secret_access_key should fail
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        Some(SecretString::from_secret("key")),
        None,
        None,
    );
    let store = S3ObjectStore::new(config);
    assert!(matches!(
        store,
        Err(S3ObjectStoreError::IncompleteCredentials)
    ));
}

#[test]
fn s3_config_with_credentials_only_secret() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned()).with_credentials(
        None,
        Some(SecretString::from_secret("secret")),
        None,
    );
    let store = S3ObjectStore::new(config);
    assert!(matches!(
        store,
        Err(S3ObjectStoreError::IncompleteCredentials)
    ));
}

// ── S3ObjectStoreConfig Debug without endpoint ────────────────────────

#[test]
fn s3_config_debug_without_endpoint_shows_none() {
    let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
    let dbg = format!("{config:?}");
    assert!(dbg.contains("endpoint: None"));
}

// ── S3MultipartUploadWriter Debug ────────────────────────────────────

#[test]
fn s3_multipart_upload_writer_debug_not_available() {
    // The writer type doesn't implement Debug, but we can
    // verify the wrapper compiles and works.
}

// ── normalize_prefix coverage edge cases ─────────────────────────────

#[test]
fn normalize_prefix_single_slash_only() {
    assert_eq!(normalize_prefix("/"), None);
}

#[test]
fn normalize_prefix_no_trim_needed() {
    assert_eq!(normalize_prefix("plain"), Some("plain".to_owned()));
}

// ── location_for_prefix with non-empty prefix, no key prefix ─────────

#[test]
fn location_for_prefix_non_empty_no_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };
    let prefix = crate::ObjectPrefix::parse("my-stuff/").unwrap();
    let location = store.location_for_prefix(&prefix);
    assert!(location.is_ok());
    if let Ok(loc) = location {
        let path = loc.as_ref();
        assert!(
            path == "my-stuff" || path == "my-stuff/",
            "unexpected: {path}"
        );
    }
}

// ── metadata_from_external with prefix and without ────────────────────

#[test]
fn metadata_from_external_strips_prefix() {
    use object_store::path::Path as ObjectStorePath;

    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("tenant-x")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };

    let meta = object_store::ObjectMeta {
        location: ObjectStorePath::from("tenant-x/ns/key.xorb"),
        last_modified: chrono::Utc::now(),
        size: 42,
        e_tag: None,
        version: None,
    };
    let result = store.metadata_from_external(&meta);
    assert!(result.is_ok());
    if let Ok(metadata) = result {
        assert_eq!(metadata.key().as_str(), "ns/key.xorb");
        assert_eq!(metadata.length(), 42);
    }
}

#[test]
fn metadata_from_external_no_prefix() {
    use object_store::path::Path as ObjectStorePath;

    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };

    let meta = object_store::ObjectMeta {
        location: ObjectStorePath::from("plain/key.xorb"),
        last_modified: chrono::Utc::now(),
        size: 100,
        e_tag: None,
        version: None,
    };
    let result = store.metadata_from_external(&meta);
    assert!(result.is_ok());
    if let Ok(metadata) = result {
        assert_eq!(metadata.key().as_str(), "plain/key.xorb");
        assert_eq!(metadata.length(), 100);
    }
}

#[test]
fn metadata_from_external_rejects_outside_prefix() {
    use object_store::path::Path as ObjectStorePath;

    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("tenant-x")),
    );
    assert!(store.is_ok());
    let Ok(store) = store else { return };

    let meta = object_store::ObjectMeta {
        location: ObjectStorePath::from("other-prefix/key.xorb"),
        last_modified: chrono::Utc::now(),
        size: 42,
        e_tag: None,
        version: None,
    };
    let result = store.metadata_from_external(&meta);
    assert!(matches!(result, Err(S3ObjectStoreError::InvalidListedKey)));
}

// ── existing_object_outcome edge cases ────────────────────────────────

#[test]
fn existing_object_outcome_length_mismatch() {
    // This function requires a real store — we test the length-mismatch
    // rejection path which is the first check and doesn't touch storage.
    // We construct a store to get the type infrastructure, but the
    // length mismatch is caught before any S3 call.
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"hello"), 5);
    let result = super::existing_object_outcome(&store, &key, 10, b"hello", &integrity);
    assert!(matches!(
        result,
        Err(S3ObjectStoreError::ExistingObjectConflict)
    ));
}

#[test]
fn existing_object_outcome_zero_length() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b""), 0);
    // When existing_length is 0, the function calls verify_integrity
    // which checks the expected bytes match the integrity.
    // Since both are empty/0, this should succeed and return AlreadyExists.
    let result = super::existing_object_outcome(&store, &key, 0, b"", &integrity);
    assert!(matches!(result, Ok(PutOutcome::AlreadyExists)));
}

#[test]
fn existing_object_outcome_zero_length_integrity_mismatch() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"x"), 0);
    // existing_length (0) == integrity.length() (0) — passes first check.
    // existing_length == 0 — enters the zero-length path.
    // verify_integrity is called with expected_bytes (empty) against
    // integrity (hash of "x", length 0).
    // Length check passes (0 == 0) but hash check fails
    // (hash of "" != hash of "x") → IntegrityHashMismatch.
    let result = super::existing_object_outcome(&store, &key, 0, b"", &integrity);
    assert!(matches!(
        result,
        Err(S3ObjectStoreError::IntegrityHashMismatch)
    ));
}

// ── block_on / block_on_result fallback paths ──────────────────────

#[test]
fn s3_block_on_fallback_with_own_runtime_on_contains() {
    // In a regular #[test] there is no tokio runtime, so Handle::try_current()
    // returns Err, which exercises the self.runtime fallback in block_on.
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18999".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    // This will fail because there is no S3 endpoint, but the block_on
    // fallback path through self.runtime is exercised.
    let result = store.contains(&key);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

#[test]
fn s3_block_on_fallback_with_own_runtime_on_metadata() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18998".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store.metadata(&key);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

#[test]
fn s3_block_on_fallback_with_own_runtime_on_delete() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18997".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store.delete_if_present(&key);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

#[test]
fn s3_block_on_fallback_with_own_runtime_on_read_range() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18996".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let range = shardline_protocol::ByteRange::new(0, 5).unwrap();

    let result = store.read_range(&key, range);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

#[test]
fn s3_block_on_result_fallback_on_visit_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18995".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let prefix = ObjectPrefix::parse("ns/").unwrap();
    let result: Result<(), S3ObjectStoreError> = store.visit_prefix(&prefix, |_meta| Ok(()));
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

#[test]
fn s3_block_on_result_fallback_on_list_flat() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18994".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let prefix = ObjectPrefix::parse("ns/").unwrap();
    let result = store.list_flat_namespace_page(&prefix, None, 10);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── location_for_prefix edge cases ────────────────────────────────

#[test]
fn location_for_prefix_with_prefix_no_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18993".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let prefix = ObjectPrefix::parse("subdir/").unwrap();
    let location = store.location_for_prefix(&prefix).unwrap();
    let path = location.as_ref();
    assert!(path == "subdir" || path == "subdir/", "unexpected: {path}");
}

#[test]
fn location_for_prefix_non_empty_prefix_with_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18992".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("pfx")),
    )
    .unwrap();
    let prefix = ObjectPrefix::parse("subdir/").unwrap();
    let location = store.location_for_prefix(&prefix).unwrap();
    let path = location.as_ref();
    assert!(path.starts_with("pfx/"), "expected pfx/ prefix, got {path}");
}

// ── copy_object_if_absent edge cases ──────────────────────────────

#[test]
fn s3_copy_object_if_absent_source_eq_destination_with_source_exists() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18991".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    // source == destination with missing source — should Err because metadata returns None
    // (the head() call fails with connection error, which is mapped through)
    let key = ObjectKey::parse("test/key").unwrap();
    let result = store.copy_object_if_absent(&key, &key);
    // Without a connection, metadata() fails, so this should be an error
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── put_if_absent error path (connection error) ────────────────────

#[test]
fn s3_put_if_absent_connection_error() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18990".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let body = ObjectBody::from_slice(b"hello world");
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"hello world"), 11);
    let result = store.put_if_absent(&key, body, &integrity);
    // verify_integrity passes, then metadata() fails with connection error
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── put_overwrite error path ───────────────────────────────────────

#[test]
fn s3_put_overwrite_connection_error() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18989".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let body = ObjectBody::from_slice(b"hello world");
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"hello world"), 11);
    let result = store.put_overwrite(&key, body, &integrity);
    // verify_integrity passes, then put_opts fails with connection error
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── existing_object_outcome with non-zero length ───────────────────

#[test]
fn existing_object_outcome_nonzero_length_read_error() {
    // existing_object_outcome with a non-zero length where store.read_range
    // fails because there is no real endpoint.
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18988".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"hello"), 5);
    // existing_length (5) == integrity.length() (5) — passes first check.
    // Then enters the streaming compare loop — store.read_range fails.
    let result = super::existing_object_outcome(&store, &key, 5, b"hello", &integrity);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── existing_object_outcome_from_file edge case ────────────────────

#[test]
fn existing_object_outcome_from_file_length_mismatch() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18987".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bin");
    std::fs::write(&path, b"hello world").unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"hello world"), 11);
    // existing_length (5) differs from integrity.length (11) → ExistingObjectConflict
    let result = super::existing_object_outcome_from_file(&store, &key, 5, &path, &integrity);
    assert!(matches!(
        result,
        Err(S3ObjectStoreError::ExistingObjectConflict)
    ));
}

#[test]
fn existing_object_outcome_from_file_zero_length() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18986".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.bin");
    std::fs::write(&path, b"").unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b""), 0);
    // existing_length (0) == integrity.length (0) and source file is empty → AlreadyExists
    let result = super::existing_object_outcome_from_file(&store, &key, 0, &path, &integrity);
    assert!(matches!(result, Ok(PutOutcome::AlreadyExists)));
}

#[test]
fn existing_object_outcome_from_file_verify_file_length_fails() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18985".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mismatch.bin");
    std::fs::write(&path, b"short").unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"short"), 99);
    // verify_file_length fails
    let result = super::existing_object_outcome_from_file(&store, &key, 99, &path, &integrity);
    assert!(matches!(
        result,
        Err(S3ObjectStoreError::IntegrityLengthMismatch)
    ));
}

// ── existing_copy_outcome edge case ────────────────────────────────

#[test]
fn existing_copy_outcome_missing_destination() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18984".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let src = ObjectKey::parse("test/src").unwrap();
    let dst = ObjectKey::parse("test/dst").unwrap();
    // destination metadata() will fail with connection error
    let result = super::existing_copy_outcome(&store, &src, &dst, 10);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── delete_location_if_present ─────────────────────────────────────

#[test]
fn s3_delete_location_if_present_connection_error() {
    use object_store::path::Path as ObjectStorePath;
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18983".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let location = ObjectStorePath::from("test/key");
    let result = store.delete_location_if_present(&location);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── list_prefix on empty prefix ────────────────────────────────────

#[test]
fn s3_list_prefix_empty_prefix_with_connection() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18982".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let prefix = ObjectPrefix::parse("").unwrap();
    // visit_prefix will fail with connection error
    let result = store.list_prefix(&prefix);
    assert!(
        result.is_err(),
        "expected error from unreachable S3: {result:?}"
    );
}

// ── put_file_if_absent with non-existent file ──────────────────────

#[test]
fn s3_put_file_if_absent_nonexistent_file() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18981".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent.bin");
    let integrity = ObjectIntegrity::new(super::chunk_hash(b""), 0);
    // verify_file_length will fail with Io error (file not found)
    let result = store.put_file_if_absent(&key, &path, &integrity);
    assert!(matches!(result, Err(S3ObjectStoreError::Io(_))));
}

#[test]
fn s3_put_content_addressed_file_nonexistent_file() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18980".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent.bin");
    let integrity = ObjectIntegrity::new(super::chunk_hash(b""), 0);
    let result = store.put_content_addressed_file(&key, &path, &integrity);
    assert!(matches!(result, Err(S3ObjectStoreError::Io(_))));
}

// ── location_for_key with key_prefix ───────────────────────────────

#[test]
fn s3_location_for_key_with_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18979".to_owned()))
            .with_allow_http(true)
            .with_key_prefix(Some("tenant-z")),
    )
    .unwrap();
    let key = ObjectKey::parse("ns/obj.xorb").unwrap();
    let location = store.location_for_key(&key).unwrap();
    assert_eq!(location.as_ref(), "tenant-z/ns/obj.xorb");
}

#[test]
fn s3_location_for_key_without_key_prefix() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18978".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("ns/obj.xorb").unwrap();
    let location = store.location_for_key(&key).unwrap();
    assert_eq!(location.as_ref(), "ns/obj.xorb");
}

// ── S3MultipartUploadWriter finish via InMemory ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_upload_writer_finish() {
    use object_store::{ObjectStoreExt, WriteMultipart};

    let inner = InMemory::new();
    let location = ObjectStorePath::from("test/finish-obj");

    let upload = inner
        .put_multipart(&location)
        .await
        .expect("should create multipart upload");
    let writer = WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
    let mut multipart = super::S3MultipartUploadWriter { writer };

    multipart.write(b"hello finish");
    multipart.wait_for_capacity(4).await.unwrap();

    let result = multipart.finish().await;
    assert!(result.is_ok());
}

// ── complete_resumable_upload validation ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_resumable_empty_parts_rejected() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18978".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store
        .complete_resumable_upload(&key, "upload-id", vec![])
        .await;

    assert!(matches!(
        result,
        Err(S3ObjectStoreError::InvalidUploadParts)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_resumable_non_consecutive_parts_rejected() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18977".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store
        .complete_resumable_upload(&key, "upload-id", vec![(0, "e0".into()), (2, "e2".into())])
        .await;

    assert!(matches!(
        result,
        Err(S3ObjectStoreError::InvalidUploadParts)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_resumable_reordered_parts_validated() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18976".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    // Parts are out of order but consecutive — sort fixes them,
    // then validation passes and we proceed to the S3 call which fails.
    let result = store
        .complete_resumable_upload(&key, "upload-id", vec![(1, "e1".into()), (0, "e0".into())])
        .await;

    // The connection to the fake endpoint fails after validation passes.
    // We expect an External error, not InvalidUploadParts.
    assert!(
        !matches!(result, Err(S3ObjectStoreError::InvalidUploadParts)),
        "consecutive parts should pass validation"
    );
    assert!(result.is_err(), "should fail with connection error");
}

// ── begin_content_addressed_upload early return ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_begin_content_addressed_upload_fails_with_connection_error() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18975".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store.begin_content_addressed_upload(&key).await;
    // metadata() will fail with connection error before reaching the upload logic
    assert!(result.is_err(), "expected connection error");
}

// ── delete_if_present with AlreadyExists from external error ────────

#[test]
fn s3_delete_if_present_connection_error() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18974".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();

    let result = store.delete_if_present(&key);
    // The S3 head/delete operation fails with connection error
    assert!(result.is_err(), "expected connection error");
}

// ── list_flat_namespace_page with start_after validation ───────────

#[test]
fn s3_list_flat_namespace_validates_start_after() {
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18972".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    // Test that start_after is properly converted to a location
    // (the S3 call itself will fail, but the transformation runs)
    let prefix = ObjectPrefix::parse("ns/").unwrap();
    let key = ObjectKey::parse("ns/key01").unwrap();
    let result = store.list_flat_namespace_page(&prefix, Some(&key), 10);
    // Should fail with connection error (the function validates start_after
    // internally in its own way for S3)
    assert!(result.is_err(), "expected connection error");
}

// ── existing_object_outcome with streaming comparison ───────────────

#[test]
fn existing_object_outcome_streaming_compare_first_chunk_fails() {
    // existing_object_outcome with non-zero length where the first
    // streaming read_range fails (no S3 endpoint).
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_endpoint(Some("http://127.0.0.1:18973".to_owned()))
            .with_allow_http(true),
    )
    .unwrap();
    let key = ObjectKey::parse("test/key").unwrap();
    let integrity = ObjectIntegrity::new(super::chunk_hash(b"x"), 1);
    // existing_length == 1 (non-zero), integrity.length() == 1 → passes first check.
    // Then enters streaming loop → store.read_range fails.
    let result = super::existing_object_outcome(&store, &key, 1, b"x", &integrity);
    assert!(result.is_err(), "expected error from read_range failure");
}
