//! Synchronous preparation of a staged native Xorb before immutable writes.

use std::{fs::File, io::BufReader, path::PathBuf};

use crate::checkpoint_reader::CheckpointReader;
use shardline_index::parse_xet_hash_hex;
use shardline_protocol::ShardlineHash;
use shardline_storage::ObjectIntegrity;

use crate::{ValidatedXorb, XetAdapterError, validate_serialized_xorb};

mod digest_reader;

pub(super) struct PreparedFile {
    pub normalized: Option<tempfile::NamedTempFile>,
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
    // Canonical input needs one complete validation, not a complete format
    // probe followed by the same validation again. Retain its serialized
    // digest from those reads; immutable storage still verifies before publish.
    if let Some(prepared) = try_prepare_canonical(expected, &path, checkpoint)? {
        return Ok(prepared);
    }
    let normalized = match ownership {
        SourceOwnership::Borrowed => {
            super::normalization::copy_footerless_with_checkpoint(expected_hash, &path, checkpoint)?
        }
        SourceOwnership::OwnedStaging => {
            // Complete validation and serialized digest extraction immediately
            // below authenticate the appended result. The public normalization
            // API separately retains its own complete-result validation.
            super::normalization::append_owned_footer(expected_hash, &path, checkpoint)?;
            None
        }
    };
    let path = normalized
        .as_ref()
        .map_or(path, |file| file.path().to_owned());
    let mut prepared = try_prepare_canonical(expected, &path, checkpoint)?
        .ok_or(XetAdapterError::InvalidSerializedXorb)?;
    prepared.normalized = normalized;
    Ok(prepared)
}

fn try_prepare_canonical(
    expected: ShardlineHash,
    path: &std::path::Path,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<Option<PreparedFile>, XetAdapterError> {
    let source_file = File::open(path)?;
    let length = source_file.metadata()?.len();
    // Footer decoding reads many small fields. Buffer the source without
    // changing the authenticated byte order or cancellation checkpoints.
    let mut reader =
        digest_reader::DigestReader::new(BufReader::with_capacity(64 * 1024, source_file))?;
    let validated = match validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut reader,
            checkpoint,
        },
        expected,
    ) {
        Ok(validated) => validated,
        // Cancellation and storage failures must never be mistaken for
        // footerless input or cause normalization to modify owned staging.
        Err(crate::XorbParseError::Io(error))
            if error.kind() != std::io::ErrorKind::UnexpectedEof =>
        {
            return Err(XetAdapterError::Io(error));
        }
        Err(_) => return Ok(None),
    };
    let (_file, digest, observed_length) = reader.finish(checkpoint)?;
    if observed_length != length {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes(*digest.as_bytes()), length);
    Ok(Some(PreparedFile {
        normalized: None,
        path: path.to_owned(),
        validated,
        integrity,
        length,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };
    use std::{cell::Cell, io::Write};

    #[cfg(target_os = "linux")]
    #[test]
    fn staged_preparation_bounds_small_read_syscalls() {
        fn read_calls() -> u64 {
            std::fs::read_to_string("/proc/thread-self/io")
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("syscr:")?.trim().parse().ok())
                .unwrap()
        }
        for footer in [true, false] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(128, ChunkSize::Fixed(65536)),
                CompressionScheme::None,
                footer,
            )
            .unwrap();
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&serialized.serialized_data).unwrap();
            let before = read_calls();
            let prepared = prepare_source(
                &serialized.hash.hex(),
                source.path().to_owned(),
                SourceOwnership::OwnedStaging,
                &|| Ok(()),
            )
            .unwrap();
            let calls = read_calls() - before;
            eprintln!("staged preparation: footer={footer}, syscr={calls}");
            assert_eq!(prepared.validated.chunks().len(), 128);
            assert!(calls < 640, "small parser reads caused {calls} syscalls");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn canonical_preparation_does_not_read_a_second_complete_container() {
        fn thread_read_characters() -> u64 {
            std::fs::read_to_string("/proc/thread-self/io")
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("rchar:")?.trim().parse().ok())
                .unwrap()
        }
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(128, ChunkSize::Fixed(65536)),
            CompressionScheme::None,
            true,
        )
        .unwrap();
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        let length = u64::try_from(serialized.serialized_data.len()).unwrap();
        for ownership in [SourceOwnership::Borrowed, SourceOwnership::OwnedStaging] {
            let before = thread_read_characters();
            let prepared = prepare_source(
                &serialized.hash.hex(),
                source.path().to_owned(),
                ownership,
                &|| Ok(()),
            )
            .unwrap();
            let reads = thread_read_characters() - before;
            eprintln!("canonical staged preparation: serialized_bytes={length}, rchar={reads}");
            assert!(prepared.normalized.is_none());
            assert_eq!(prepared.length, length);
            assert_eq!(
                prepared.integrity.hash(),
                ShardlineHash::from_bytes(*blake3::hash(&serialized.serialized_data).as_bytes())
            );
            assert!(
                reads < length * 3 / 2,
                "canonical preparation read {reads} bytes for {length} serialized bytes"
            );
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn owned_footerless_preparation_does_not_validate_the_canonical_payload_twice() {
        fn thread_read_characters() -> u64 {
            std::fs::read_to_string("/proc/thread-self/io")
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("rchar:")?.trim().parse().ok())
                .unwrap()
        }
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(128, ChunkSize::Fixed(65536)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        let length = u64::try_from(serialized.serialized_data.len()).unwrap();
        let before = thread_read_characters();
        let prepared = prepare_source(
            &serialized.hash.hex(),
            source.path().to_owned(),
            SourceOwnership::OwnedStaging,
            &|| Ok(()),
        )
        .unwrap();
        let reads = thread_read_characters() - before;
        eprintln!("owned footerless preparation: wire_bytes={length}, rchar={reads}");
        assert!(prepared.normalized.is_none());
        assert_eq!(prepared.path, source.path());
        assert_eq!(
            prepared.integrity.hash(),
            ShardlineHash::from_bytes(
                *blake3::hash(&std::fs::read(source.path()).unwrap()).as_bytes()
            )
        );
        assert!(
            reads < length * 5 / 2,
            "owned footerless preparation read {reads} bytes for {length} wire bytes"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn borrowed_footerless_preparation_avoids_a_third_payload_read() {
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(128, ChunkSize::Fixed(65536)),
            CompressionScheme::None,
            false,
        )
        .unwrap();
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&serialized.serialized_data).unwrap();
        let read_characters = || {
            std::fs::read_to_string("/proc/thread-self/io")
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("rchar:")?.trim().parse::<u64>().ok())
                .unwrap()
        };
        let length = serialized.serialized_data.len() as u64;
        let before = read_characters();
        let prepared =
            prepare(&serialized.hash.hex(), source.path().to_owned(), &|| Ok(())).unwrap();
        let reads = read_characters().checked_sub(before).unwrap();
        assert!(prepared.normalized.is_some());
        assert!(
            reads < length * 5 / 2,
            "read {reads} bytes for {length} input bytes"
        );
        assert_eq!(
            std::fs::read(source.path()).unwrap(),
            serialized.serialized_data
        );
        assert_eq!(
            prepared.integrity.hash(),
            ShardlineHash::from_bytes(
                *blake3::hash(&std::fs::read(prepared.path).unwrap()).as_bytes()
            )
        );
    }

    #[test]
    fn owned_footerless_preparation_rejects_mutation_after_footer_append() {
        use std::io::{Seek, SeekFrom};
        for truncate in [false, true] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(4, ChunkSize::Fixed(4096)),
                CompressionScheme::None,
                false,
            )
            .unwrap();
            let mut source = tempfile::NamedTempFile::new().unwrap();
            source.write_all(&serialized.serialized_data).unwrap();
            let length = serialized.serialized_data.len() as u64;
            let changed = Cell::new(false);
            let result = prepare_source(
                &serialized.hash.hex(),
                source.path().to_owned(),
                SourceOwnership::OwnedStaging,
                &|| {
                    if !changed.get() && source.as_file().metadata()?.len() > length {
                        changed.set(true);
                        if truncate {
                            source.as_file().set_len(length)?;
                        } else {
                            let mut file = std::fs::OpenOptions::new()
                                .write(true)
                                .open(source.path())?;
                            file.seek(SeekFrom::Start(8))?;
                            file.write_all(&[serialized.serialized_data[8] ^ 1])?;
                        }
                    }
                    Ok(())
                },
            );
            assert!(
                changed.get(),
                "fixture must mutate after appending the footer"
            );
            assert!(
                result.is_err(),
                "changed staging must not become publishable"
            );
        }
    }

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
