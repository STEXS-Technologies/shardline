//! Synchronous preparation of a staged native Xorb before immutable writes.

use std::{fs::File, path::PathBuf};

use crate::checkpoint_reader::CheckpointReader;
use shardline_index::parse_xet_hash_hex;
use shardline_protocol::ShardlineHash;
use shardline_storage::ObjectIntegrity;

use crate::{ValidatedXorb, XetAdapterError, validate_serialized_xorb};

mod digest_reader;

pub(super) struct PreparedFile {
    pub normalized: Option<tempfile::NamedTempFile>,
    pub file: File,
    pub path: PathBuf,
    pub validated: ValidatedXorb,
    pub integrity: ObjectIntegrity,
    pub length: u64,
}

#[derive(Clone, Copy)]
pub(super) enum SourceOwnership {
    Borrowed,
    OwnedStaging,
}

#[cfg(test)]
pub(super) fn prepare(
    expected_hash: &str,
    path: PathBuf,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<PreparedFile, XetAdapterError> {
    prepare_source(expected_hash, path, SourceOwnership::Borrowed, checkpoint)
}

pub(super) fn prepare_source(
    expected_hash: &str,
    path: PathBuf,
    ownership: SourceOwnership,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<PreparedFile, XetAdapterError> {
    checkpoint()?;
    let expected = parse_xet_hash_hex(expected_hash)?;
    let normalized = match ownership {
        SourceOwnership::Borrowed => {
            super::normalization::normalize_with_checkpoint(expected_hash, &path, checkpoint)?
        }
        SourceOwnership::OwnedStaging => {
            super::normalization::normalize_owned_with_checkpoint(
                expected_hash,
                &path,
                checkpoint,
            )?;
            None
        }
    };
    let path = normalized
        .as_ref()
        .map_or(path, |file| file.path().to_owned());
    let source_file = File::open(&path)?;
    let length = source_file.metadata()?.len();
    let mut reader = digest_reader::DigestReader::new(source_file)?;
    let validated = validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut reader,
            checkpoint,
        },
        expected,
    )?;
    let (file, digest, observed_length) = reader.finish(checkpoint)?;
    if observed_length != length {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes(*digest.as_bytes()), length);
    Ok(PreparedFile {
        normalized,
        file,
        path,
        validated,
        integrity,
        length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };
    use std::{cell::Cell, io::Write};

    #[test]
    fn owned_preparation_cancellation_never_publishes_and_staging_remains_retryable() {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(4, ChunkSize::Fixed(4096)),
            CompressionScheme::LZ4,
            false,
        )
        .unwrap();
        let hash = serialized.hash.hex();
        let mut complete = tempfile::NamedTempFile::new().unwrap();
        complete.write_all(&serialized.serialized_data).unwrap();
        let completed = Cell::new(0);
        let prepared = prepare_source(
            &hash,
            complete.path().to_owned(),
            SourceOwnership::OwnedStaging,
            &|| {
                completed.set(completed.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert!(prepared.normalized.is_none());
        assert_eq!(prepared.path, complete.path());
        for limit in 1..=completed.get() {
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&serialized.serialized_data).unwrap();
            let calls = Cell::new(0);
            assert!(
                prepare_source(
                    &hash,
                    source.path().to_owned(),
                    SourceOwnership::OwnedStaging,
                    &|| {
                        calls.set(calls.get() + 1);
                        if calls.get() >= limit {
                            Err(std::io::ErrorKind::ConnectionAborted.into())
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err(),
                "checkpoint {limit} returned publishable content"
            );
            // Cancellation before append leaves footerless input; after append
            // leaves canonical input. Both must pass complete revalidation.
            assert!(
                prepare_source(
                    &hash,
                    source.path().to_owned(),
                    SourceOwnership::OwnedStaging,
                    &|| Ok(())
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn cancelled_canonical_and_footerless_preparation_never_returns_a_publishable_result() {
        for footer in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(8, ChunkSize::Fixed(128 * 1024)),
                CompressionScheme::None,
                footer,
            )
            .unwrap();
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&serialized.serialized_data).unwrap();
            let hash = serialized.hash.hex();
            let calls = Cell::new(0);
            let checkpoint = || {
                calls.set(calls.get() + 1);
                if calls.get() >= 4 {
                    Err(std::io::ErrorKind::ConnectionAborted.into())
                } else {
                    Ok(())
                }
            };
            assert!(prepare(&hash, source.path().to_owned(), &checkpoint).is_err());
            assert!(
                calls.get() <= 5,
                "cancellation must not fall through into normalization"
            );
            assert_eq!(
                std::fs::read(source.path()).unwrap(),
                serialized.serialized_data
            );
            let completed = Cell::new(0);
            assert!(
                prepare(&hash, source.path().to_owned(), &|| {
                    completed.set(completed.get() + 1);
                    Ok(())
                })
                .is_ok()
            );
            // Also cancel during the final digest pass and immediately before
            // returning its result, rather than testing only the parser entry.
            for limit in [completed.get() - 1, completed.get()] {
                let calls = Cell::new(0);
                let checkpoint = || {
                    calls.set(calls.get() + 1);
                    if calls.get() >= limit {
                        Err(std::io::ErrorKind::ConnectionAborted.into())
                    } else {
                        Ok(())
                    }
                };
                assert!(matches!(
                    prepare(&hash, source.path().to_owned(), &checkpoint),
                    Err(XetAdapterError::Io(error)) if error.kind() == std::io::ErrorKind::ConnectionAborted
                ));
            }
        }
    }
}
