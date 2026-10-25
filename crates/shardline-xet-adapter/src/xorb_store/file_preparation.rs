//! Synchronous preparation of a staged native Xorb before immutable writes.

use std::{fs::File, path::PathBuf};

use crate::checkpoint_reader::CheckpointReader;
use shardline_index::parse_xet_hash_hex;
use shardline_protocol::ShardlineHash;
use shardline_storage::ObjectIntegrity;

use crate::{ValidatedXorb, XetAdapterError, validate_serialized_xorb};

pub(super) struct PreparedFile {
    pub normalized: Option<tempfile::NamedTempFile>,
    pub file: File,
    pub path: PathBuf,
    pub validated: ValidatedXorb,
    pub integrity: ObjectIntegrity,
    pub length: u64,
}

pub(super) fn prepare(
    expected_hash: &str,
    path: PathBuf,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<PreparedFile, XetAdapterError> {
    checkpoint()?;
    let expected = parse_xet_hash_hex(expected_hash)?;
    let normalized =
        super::normalization::normalize_with_checkpoint(expected_hash, &path, checkpoint)?;
    let path = normalized
        .as_ref()
        .map_or(path, |file| file.path().to_owned());
    let mut file = File::open(&path)?;
    let length = file.metadata()?.len();
    let validated = validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut file,
            checkpoint,
        },
        expected,
    )?;
    let mut hash_source = File::open(&path)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(
        &mut CheckpointReader {
            reader: &mut hash_source,
            checkpoint,
        },
        &mut hasher,
    )?;
    checkpoint()?;
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*hasher.finalize().as_bytes()),
        length,
    );
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
