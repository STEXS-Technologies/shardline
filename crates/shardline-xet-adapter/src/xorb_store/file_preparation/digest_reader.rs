//! Reuse contiguous serialized bytes read by validation for object integrity.
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};

use crate::checkpoint_reader::CheckpointReader;

pub(super) struct DigestReader<R> {
    inner: R,
    position: u64,
    hashed: u64,
    digest: BufWriter<blake3::Hasher>,
}

impl<R: Read + Seek> DigestReader<R> {
    pub(super) fn new(mut inner: R) -> io::Result<Self> {
        inner.seek(SeekFrom::Start(0))?;
        Ok(Self {
            inner,
            position: 0,
            hashed: 0,
            // Validation reads small wire fields. Batch those bytes so hashing
            // retains SIMD efficiency without a second complete file read.
            digest: BufWriter::with_capacity(64 * 1024, blake3::Hasher::new()),
        })
    }

    pub(super) fn finish(
        mut self,
        checkpoint: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<(R, blake3::Hash, u64)> {
        checkpoint()?;
        // Footer-first probes and arbitrary seeks must not reorder the digest.
        // Only the contiguous prefix was hashed; read every remaining byte.
        self.seek(SeekFrom::Start(self.hashed))?;
        io::copy(
            &mut CheckpointReader {
                reader: &mut self,
                checkpoint,
            },
            &mut io::sink(),
        )?;
        checkpoint()?;
        let digest = self.digest.into_inner().map_err(io::Error::other)?;
        Ok((self.inner, digest.finalize(), self.hashed))
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        let end = self
            .position
            .checked_add(u64::try_from(count).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("serialized digest position overflow"))?;
        if self.position <= self.hashed && end > self.hashed {
            let offset = self
                .hashed
                .checked_sub(self.position)
                .ok_or_else(|| io::Error::other("serialized digest prefix underflow"))?;
            let start = usize::try_from(offset).map_err(io::Error::other)?;
            let suffix = buffer
                .get(start..count)
                .ok_or_else(|| io::Error::other("serialized digest read bounds"))?;
            self.digest.write_all(suffix)?;
            self.hashed = end;
        }
        self.position = end;
        Ok(count)
    }
}

impl<R: Seek> Seek for DigestReader<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let actual = self.inner.seek(position)?;
        self.position = actual;
        Ok(actual)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, io::Cursor, rc::Rc};

    struct Counted {
        inner: Cursor<Vec<u8>>,
        bytes: Rc<Cell<usize>>,
    }

    impl Read for Counted {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            // Exercise short reads independently of caller buffer size.
            let limit = buffer.len().min(17);
            let count = self.inner.read(&mut buffer[..limit])?;
            self.bytes.set(self.bytes.get().checked_add(count).unwrap());
            Ok(count)
        }
    }

    impl Seek for Counted {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.inner.seek(position)
        }
    }

    #[test]
    fn footer_probes_overlaps_and_gaps_preserve_exact_digest_without_prefix_reread() {
        let data: Vec<_> = (0..4096).map(|i| (i % 251) as u8).collect();
        let bytes = Rc::new(Cell::new(0));
        let mut reader = DigestReader::new(Counted {
            inner: Cursor::new(data.clone()),
            bytes: bytes.clone(),
        })
        .unwrap();
        let mut probe = [0; 13];
        reader.seek(SeekFrom::End(-13)).unwrap();
        reader.read_exact(&mut probe).unwrap();
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut [0; 100]).unwrap();
        reader.seek(SeekFrom::Start(90)).unwrap();
        reader.read_exact(&mut [0; 30]).unwrap();
        assert_eq!(reader.hashed, 120);
        reader.seek(SeekFrom::Start(1024)).unwrap();
        reader.read_exact(&mut probe).unwrap();
        assert_eq!(reader.hashed, 120, "a gap must not enter the digest");
        let before = bytes.get();
        let (_, digest, length) = reader.finish(&|| Ok(())).unwrap();
        assert_eq!(digest, blake3::hash(&data));
        assert_eq!(length, data.len() as u64);
        assert_eq!(bytes.get() - before, data.len() - 120);
    }

    #[test]
    fn buffered_digest_preserves_hash_across_flush_boundaries() {
        for length in [65_535, 65_536, 65_537, 131_073] {
            let data: Vec<_> = (0..length).map(|i| (i % 251) as u8).collect();
            let mut reader = DigestReader::new(Cursor::new(data.clone())).unwrap();
            let mut scratch = [0; 19];
            while reader.read(&mut scratch).unwrap() != 0 {}
            let (_, digest, observed) = reader.finish(&|| Ok(())).unwrap();
            assert_eq!(digest, blake3::hash(&data));
            assert_eq!(observed, length as u64);
        }
    }

    #[test]
    fn actual_xorb_validation_reuses_payload_and_finishes_the_exact_wire_digest() {
        use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };
        for compression in [CompressionScheme::None, CompressionScheme::LZ4] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(8, ChunkSize::Fixed(4096)),
                compression,
                true,
            )
            .unwrap();
            let bytes = Rc::new(Cell::new(0));
            let mut reader = DigestReader::new(Counted {
                inner: Cursor::new(serialized.serialized_data.clone()),
                bytes: bytes.clone(),
            })
            .unwrap();
            crate::validate_serialized_xorb(
                &mut reader,
                shardline_index::parse_xet_hash_hex(&serialized.hash.hex()).unwrap(),
            )
            .unwrap();
            let prefix = usize::try_from(reader.hashed).unwrap();
            assert!(
                prefix > 0,
                "real validation must provide reusable wire bytes"
            );
            let before = bytes.get();
            let (_, digest, length) = reader.finish(&|| Ok(())).unwrap();
            assert_eq!(digest, blake3::hash(&serialized.serialized_data));
            assert_eq!(length, serialized.serialized_data.len() as u64);
            assert_eq!(
                bytes.get().checked_sub(before).unwrap(),
                serialized
                    .serialized_data
                    .len()
                    .checked_sub(prefix)
                    .unwrap()
            );
        }
    }

    #[test]
    fn changed_or_truncated_hashed_source_cannot_pass_final_immutable_integrity() {
        use shardline_protocol::ShardlineHash;
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
        for truncate in [false, true] {
            let original = vec![42; 128];
            let mut reader = DigestReader::new(Cursor::new(original)).unwrap();
            reader.read_exact(&mut [0; 128]).unwrap();
            if truncate {
                reader.inner.get_mut().truncate(64);
            } else {
                *reader.inner.get_mut().first_mut().unwrap() = 99;
            }
            let (source, digest, length) = reader.finish(&|| Ok(())).unwrap();
            let root = tempfile::tempdir().unwrap();
            let objects = shardline_server_core::ServerObjectStore::local(root.path()).unwrap();
            let key = ObjectKey::parse("xorbs/default/changed.xorb").unwrap();
            let integrity =
                ObjectIntegrity::new(ShardlineHash::from_bytes(*digest.as_bytes()), length);
            assert!(
                ObjectStore::put_if_absent(
                    &objects,
                    &key,
                    ObjectBody::from_vec(source.into_inner()),
                    &integrity,
                )
                .is_err()
            );
            assert!(ObjectStore::metadata(&objects, &key).unwrap().is_none());
        }
    }

    #[test]
    fn cancellation_during_remaining_suffix_never_returns_integrity() {
        let mut reader = DigestReader::new(Cursor::new(vec![42; 65536])).unwrap();
        reader.read_exact(&mut [0; 17]).unwrap();
        let calls = Cell::new(0);
        let result = reader.finish(&|| {
            calls.set(calls.get() + 1);
            if calls.get() >= 3 {
                Err(io::ErrorKind::ConnectionAborted.into())
            } else {
                Ok(())
            }
        });
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::ConnectionAborted));
    }
}
