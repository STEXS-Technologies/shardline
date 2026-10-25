//! File-backed normalization of standard footerless Xorbs.

#[cfg(all(test, target_os = "linux"))]
mod io_qualification;

use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::Path,
};

use shardline_index::parse_xet_hash_hex;
use shardline_xet_core::error::CoreError;
use shardline_xet_core::xorb_object::{
    deserialize_chunk_header, reconstruct_xorb_with_footer_from_reader,
};

use crate::{XetAdapterError, validate_serialized_xorb};

/// Returns a verified normalized temporary file only when the source needs it.
/// Canonical input is read in place. The source is never modified.
///
/// # Errors
/// Returns an error for invalid content, over-limit input, I/O, or hash mismatch.
pub fn normalize_uploaded_xorb_file(
    expected_hash: &str,
    path: &Path,
) -> Result<Option<tempfile::NamedTempFile>, XetAdapterError> {
    normalize_with_checkpoint(expected_hash, path, &|| Ok(()))
}

pub(super) fn normalize_with_checkpoint(
    expected_hash: &str,
    path: &Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<Option<tempfile::NamedTempFile>, XetAdapterError> {
    use crate::checkpoint_reader::CheckpointReader;
    let Some(mut input) = footerless_input(expected_hash, path, checkpoint)? else {
        return Ok(None);
    };
    let expected = parse_xet_hash_hex(expected_hash)?;
    let length = input.metadata()?.len();
    let mut normalized = tempfile::NamedTempFile::new()?;
    let (_, computed) = reconstruct_xorb_with_footer_from_reader(
        normalized.as_file_mut(),
        &mut CheckpointReader {
            reader: &mut input,
            checkpoint,
        },
        length,
    )
    .map_err(normalization_error)?;
    if parse_xet_hash_hex(&computed.hex())? != expected {
        return Err(XetAdapterError::XorbHashMismatch);
    }
    checkpoint()?;
    validate_serialized_xorb(
        &mut CheckpointReader {
            reader: normalized.as_file_mut(),
            checkpoint,
        },
        expected,
    )?;
    Ok(Some(normalized))
}

/// Only privately owned upload staging may be modified. Authenticate every
/// member/root before writing the footer; borrowed input keeps the copy path.
pub(super) fn normalize_owned_with_checkpoint(
    expected_hash: &str,
    path: &Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<(), XetAdapterError> {
    use crate::checkpoint_reader::CheckpointReader;
    use shardline_xet_core::xorb_object::{XorbObject, inspect_footerless_xorb_from_reader};
    let Some(mut input) = footerless_input(expected_hash, path, checkpoint)? else {
        return Ok(());
    };
    let expected = parse_xet_hash_hex(expected_hash)?;
    let length = input.metadata()?.len();
    let info = inspect_footerless_xorb_from_reader(
        &mut CheckpointReader {
            reader: &mut input,
            checkpoint,
        },
        length,
    )
    .map_err(normalization_error)?;
    if parse_xet_hash_hex(&info.xorb_hash.hex())? != expected {
        return Err(XetAdapterError::XorbHashMismatch);
    }
    checkpoint()?;
    let mut output = std::fs::OpenOptions::new().append(true).open(path)?;
    if output.metadata()?.len() != length {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    XorbObject::serialize_given_info(&mut output, info).map_err(normalization_error)?;
    checkpoint()?;
    validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut input,
            checkpoint,
        },
        expected,
    )?;
    Ok(())
}

fn footerless_input(
    expected_hash: &str,
    path: &Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<Option<File>, XetAdapterError> {
    use crate::checkpoint_reader::CheckpointReader;
    checkpoint()?;
    let expected = parse_xet_hash_hex(expected_hash)?;
    let mut input = File::open(path)?;
    match validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut input,
            checkpoint,
        },
        expected,
    ) {
        Ok(_) => return Ok(None),
        // An incomplete canonical footer can be valid footerless input. Reader
        // failures and cancellation are not format detection: never consume
        // them and continue work or append to owned staging.
        Err(crate::XorbParseError::Io(error))
            if error.kind() != std::io::ErrorKind::UnexpectedEof =>
        {
            return Err(XetAdapterError::Io(error));
        }
        Err(error) => tracing::debug!(%error, "trying bounded footerless Xorb normalization"),
    }
    checkpoint()?;
    let length = input.metadata()?.len();
    // Standard Xet Xorbs contain at most 64 MiB and 8,192 chunks. Preserve the
    // wider canonical-input acceptance; these bounds apply to normalization.
    const MAX_FOOTERLESS_BYTES: u64 = 64 * 1024 * 1024 + 8192 * 8;
    const MAX_FOOTERLESS_DECODED: u64 = 64 * 1024 * 1024;
    if length > MAX_FOOTERLESS_BYTES {
        tracing::debug!(
            length,
            maximum = MAX_FOOTERLESS_BYTES,
            "footerless Xorb wire size exceeded normalization limit"
        );
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    input.seek(SeekFrom::Start(0))?;
    let mut decoded = 0_u64;
    let mut count = 0_usize;
    while input.stream_position()? < length {
        checkpoint()?;
        // Only a complete footerless chunk stream is eligible for repair.
        // In particular, never discard an invalid footer and authenticate only
        // the payload prefix that happened to precede it.
        let header = deserialize_chunk_header(&mut input).map_err(normalization_error)?;
        count = count.checked_add(1).ok_or(XetAdapterError::Overflow)?;
        if count > shardline_index::MAX_XET_DESCRIPTOR_CHUNKS {
            return Err(XetAdapterError::TooManyShardTerms);
        }
        decoded = decoded
            .checked_add(u64::from(header.get_uncompressed_length()))
            .ok_or(XetAdapterError::Overflow)?;
        if decoded > MAX_FOOTERLESS_DECODED {
            tracing::debug!(
                decoded,
                count,
                maximum = MAX_FOOTERLESS_DECODED,
                "footerless Xorb decoded size exceeded normalization limit"
            );
            return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
        }
        let end = input
            .stream_position()?
            .checked_add(u64::from(header.get_compressed_length()))
            .ok_or(XetAdapterError::Overflow)?;
        if end > length {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        input.seek(SeekFrom::Start(end))?;
    }
    Ok(Some(input))
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "CoreError is non-exhaustive; unknown parser errors must remain invalid input"
)]
fn normalization_error(error: CoreError) -> XetAdapterError {
    match error {
        // A truncated client frame is invalid input. Quota, disk, permission,
        // and other storage failures must remain server errors, so clients can
        // retry them instead of treating valid Xorb content as malformed.
        CoreError::Io(error) if error.kind() != std::io::ErrorKind::UnexpectedEof => {
            XetAdapterError::Io(error)
        }
        error => {
            tracing::debug!(%error, "footerless Xorb normalization rejected input");
            XetAdapterError::InvalidSerializedXorb
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };
    use std::io::{Cursor, Write};

    #[test]
    fn cancelled_canonical_probe_never_retries_as_footerless_or_mutates_staging() {
        use std::cell::Cell;
        for footer in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(2, ChunkSize::Fixed(4096)),
                CompressionScheme::LZ4,
                footer,
            )
            .unwrap();
            for owned in [false, true] {
                let mut source = tempfile::NamedTempFile::new().unwrap();
                source.write_all(&serialized.serialized_data).unwrap();
                let calls = Cell::new(0);
                let checkpoint = || {
                    let count = calls.get() + 1;
                    calls.set(count);
                    if count == 2 {
                        // A terminal cancellation must not require the caller
                        // to repeat the same error at another checkpoint.
                        Err(std::io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                };
                let hash = serialized.hash.hex();
                let result = if owned {
                    normalize_owned_with_checkpoint(&hash, source.path(), &checkpoint)
                } else {
                    normalize_with_checkpoint(&hash, source.path(), &checkpoint).map(|_| ())
                };
                assert!(
                    matches!(result, Err(XetAdapterError::Io(ref error))
                    if error.kind() == std::io::ErrorKind::ConnectionAborted),
                    "cancelled normalization continued or changed the error: {result:?}"
                );
                assert_eq!(
                    calls.get(),
                    2,
                    "normalization must stop at its failed checkpoint"
                );
                assert_eq!(
                    std::fs::read(source.path()).unwrap(),
                    serialized.serialized_data
                );
                normalize_owned_with_checkpoint(&hash, source.path(), &|| Ok(())).unwrap();
            }
        }
    }

    #[test]
    fn owned_normalization_matches_borrowed_bytes_and_rejects_before_mutation() {
        for scheme in [
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
        ] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(8, ChunkSize::Fixed(4096)),
                scheme,
                false,
            )
            .unwrap();
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&serialized.serialized_data).unwrap();
            let hash = serialized.hash.hex();
            let borrowed = normalize_uploaded_xorb_file(&hash, source.path())
                .unwrap()
                .unwrap();
            assert_eq!(
                std::fs::read(source.path()).unwrap(),
                serialized.serialized_data
            );
            let wrong_hash = "00".repeat(32);
            assert!(
                normalize_owned_with_checkpoint(&wrong_hash, source.path(), &|| Ok(())).is_err()
            );
            assert_eq!(
                std::fs::read(source.path()).unwrap(),
                serialized.serialized_data
            );
            assert!(
                normalize_owned_with_checkpoint(&hash, source.path(), &|| Err(
                    std::io::ErrorKind::ConnectionAborted.into()
                ))
                .is_err()
            );
            assert_eq!(
                std::fs::read(source.path()).unwrap(),
                serialized.serialized_data
            );
            normalize_owned_with_checkpoint(&hash, source.path(), &|| Ok(())).unwrap();
            let canonical = std::fs::read(source.path()).unwrap();
            assert_eq!(canonical, std::fs::read(borrowed.path()).unwrap());
            assert!(canonical.starts_with(&serialized.serialized_data));
            normalize_owned_with_checkpoint(&hash, source.path(), &|| Ok(())).unwrap();
            assert_eq!(std::fs::read(source.path()).unwrap(), canonical);
            for malformed in [
                serialized.serialized_data[..serialized.serialized_data.len() - 1].to_vec(),
                [serialized.serialized_data.clone(), vec![255; 8]].concat(),
            ] {
                let mut invalid = tempfile::NamedTempFile::new().unwrap();
                invalid.write_all(&malformed).unwrap();
                assert!(
                    normalize_owned_with_checkpoint(&hash, invalid.path(), &|| Ok(())).is_err()
                );
                assert_eq!(std::fs::read(invalid.path()).unwrap(), malformed);
            }
        }
    }

    #[test]
    fn normalization_preserves_output_quota_failures_as_storage_errors() {
        struct QuotaWriter;
        impl Write for QuotaWriter {
            fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::QuotaExceeded,
                    "fixture quota",
                ))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(1, ChunkSize::Fixed(1024)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        let error = reconstruct_xorb_with_footer_from_reader(
            &mut QuotaWriter,
            &mut Cursor::new(&serialized.serialized_data),
            serialized.serialized_data.len() as u64,
        )
        .unwrap_err();
        assert!(
            matches!(normalization_error(error), XetAdapterError::Io(error)
            if error.kind() == std::io::ErrorKind::QuotaExceeded)
        );
    }

    #[tokio::test]
    async fn owned_upload_releases_staging_and_admission_after_success_or_rejection() {
        use shardline_server_core::ServerObjectStore;
        use shardline_storage::ObjectStore;
        use std::sync::Arc;

        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(2, ChunkSize::Fixed(128)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let mut input = tempfile::NamedTempFile::new().unwrap();
        input.write_all(&serialized.serialized_data).unwrap();
        let source = input.path().to_owned();
        let permit = admission.clone().acquire_owned().await.unwrap();
        let stored = super::super::store_owned_xorb_file(&objects, &hash, input, permit)
            .await
            .unwrap();
        assert!(stored.was_inserted);
        assert!(!source.exists());
        assert_eq!(admission.available_permits(), 1);
        let key = super::super::xorb_object_key(&hash).unwrap();
        let metadata = ObjectStore::metadata(&objects, &key).unwrap().unwrap();
        let original =
            shardline_server_core::read_full_object(&objects, &key, metadata.length()).unwrap();

        let mut damaged = original.clone();
        *damaged.last_mut().unwrap() ^= 0xff;
        let mut invalid = tempfile::NamedTempFile::new().unwrap();
        invalid.write_all(&damaged).unwrap();
        let invalid_source = invalid.path().to_owned();
        let retry_permit = admission.clone().acquire_owned().await.unwrap();
        assert!(
            super::super::store_owned_xorb_file(&objects, &hash, invalid, retry_permit)
                .await
                .is_err()
        );
        assert!(!invalid_source.exists());
        assert_eq!(admission.available_permits(), 1);
        assert_eq!(
            shardline_server_core::read_full_object(&objects, &key, metadata.length()).unwrap(),
            original
        );
    }

    #[test]
    fn normalization_rejects_trailing_garbage_and_damaged_canonical_footers() {
        for footer in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(2, ChunkSize::Fixed(128)),
                CompressionScheme::None,
                footer,
            )
            .unwrap();
            let hash = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();
            let mut malformed = serialized.serialized_data;
            if footer {
                *malformed.last_mut().unwrap() ^= 0xff;
            } else {
                malformed.extend_from_slice(&[0xff; 8]);
            }
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&malformed).unwrap();
            assert!(
                normalize_uploaded_xorb_file(&serialized.hash.hex(), source.path()).is_err(),
                "file normalization accepted malformed input (footer={footer})"
            );
            assert!(
                super::super::normalize_serialized_xorb(hash, &malformed).is_err(),
                "memory normalization accepted malformed input (footer={footer})"
            );
            assert_eq!(std::fs::read(source.path()).unwrap(), malformed);
        }
    }

    #[test]
    fn footerless_exact_decoded_and_member_limits_are_accepted() {
        // The two limits coincide here: 8,192 members, each 8 KiB, plus
        // exactly the permitted 8-byte header overhead per member.
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(8192, ChunkSize::Fixed(8192)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        assert_eq!(
            serialized.serialized_data.len(),
            64 * 1024 * 1024 + 8192 * 8
        );
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        let normalized = normalize_uploaded_xorb_file(&serialized.hash.hex(), source.path())
            .unwrap()
            .unwrap();
        let validated = validate_serialized_xorb(
            &mut File::open(normalized.path()).unwrap(),
            parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
        )
        .unwrap();
        assert_eq!(validated.unpacked_length(), 64 * 1024 * 1024);
        assert_eq!(validated.chunks().len(), 8192);
        let output_path = normalized.path().to_path_buf();
        drop(normalized);
        assert!(
            !output_path.exists(),
            "normalization scratch survived its owner"
        );
        assert_eq!(
            std::fs::read(source.path()).unwrap(),
            serialized.serialized_data
        );
    }

    #[test]
    fn footerless_member_limit_is_checked_before_reconstruction() {
        use shardline_xet_core::xorb_object::{XorbChunkHeader, write_chunk_header};
        let mut source = tempfile::NamedTempFile::new().unwrap();
        for _ in 0..8193 {
            write_chunk_header(
                &mut source,
                &XorbChunkHeader::new(CompressionScheme::None, 1, 1),
            )
            .unwrap();
            source.write_all(&[1]).unwrap();
        }
        assert!(matches!(
            normalize_uploaded_xorb_file(&"0".repeat(64), source.path()),
            Err(XetAdapterError::TooManyShardTerms)
        ));
    }

    #[test]
    fn footerless_one_byte_over_size_limits_is_rejected_before_reconstruction() {
        use shardline_xet_core::xorb_object::{XorbChunkHeader, write_chunk_header};
        let mut decoded = tempfile::NamedTempFile::new().unwrap();
        for length in std::iter::repeat_n(128 * 1024, 512).chain(std::iter::once(1)) {
            write_chunk_header(
                &mut decoded,
                &XorbChunkHeader::new(CompressionScheme::LZ4, 1, length),
            )
            .unwrap();
            decoded.write_all(&[0]).unwrap();
        }
        assert!(matches!(
            normalize_uploaded_xorb_file(&"0".repeat(64), decoded.path()),
            Err(XetAdapterError::XorbUnpackedLengthExceedsCap)
        ));
        let oversized = tempfile::NamedTempFile::new().unwrap();
        oversized
            .as_file()
            .set_len(64 * 1024 * 1024 + 8192 * 8 + 1)
            .unwrap();
        assert!(matches!(
            normalize_uploaded_xorb_file(&"0".repeat(64), oversized.path()),
            Err(XetAdapterError::InvalidSerializedXorb)
        ));
    }

    #[test]
    fn large_footerless_normalization_preserves_payload_and_canonical_bytes() {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(144, ChunkSize::Fixed(128 * 1024)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        assert!(serialized.serialized_data.len() > 16 * 1024 * 1024);
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        let normalized = normalize_uploaded_xorb_file(&serialized.hash.hex(), source.path())
            .unwrap()
            .unwrap();
        let actual = std::fs::read(normalized.path()).unwrap();
        let expected = super::super::normalize_serialized_xorb(
            parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
            &serialized.serialized_data,
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            std::fs::read(source.path()).unwrap(),
            serialized.serialized_data,
            "source bytes must remain untouched"
        );
        assert!(
            normalize_uploaded_xorb_file(&serialized.hash.hex(), normalized.path())
                .unwrap()
                .is_none()
        );
        assert!(
            validate_serialized_xorb(
                &mut Cursor::new(actual),
                parse_xet_hash_hex(&serialized.hash.hex()).unwrap()
            )
            .is_ok()
        );
    }

    #[test]
    fn file_normalization_rejects_wrong_identity_truncation_and_excessive_decoded_size() {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(1, ChunkSize::Fixed(128)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        assert!(normalize_uploaded_xorb_file(&"0".repeat(64), source.path()).is_err());
        source
            .as_file_mut()
            .set_len(u64::try_from(serialized.serialized_data.len()).unwrap() - 1)
            .unwrap();
        assert!(normalize_uploaded_xorb_file(&serialized.hash.hex(), source.path()).is_err());
        let mut bomb = tempfile::NamedTempFile::new().unwrap();
        // Valid bounded chunk headers declaring more than 64 MiB in total;
        // no decompression or output-file allocation may precede this rejection.
        use shardline_xet_core::xorb_object::{XorbChunkHeader, write_chunk_header};
        for _ in 0..1025 {
            write_chunk_header(
                &mut bomb,
                &XorbChunkHeader::new(CompressionScheme::LZ4, 1, 64 * 1024),
            )
            .unwrap();
            bomb.write_all(&[0]).unwrap();
        }
        assert!(matches!(
            normalize_uploaded_xorb_file(&serialized.hash.hex(), bomb.path()),
            Err(XetAdapterError::XorbUnpackedLengthExceedsCap)
        ));
    }
}
