//! Derive and verify file SHA-256 identities from authenticated Xorb members.

use std::io::{Seek, SeekFrom};

use sha2::{Digest, Sha256};
use shardline_server_core::MaterializedObject;
use shardline_xet_core::xorb_object::deserialize_chunk;

use super::*;

/// Keep one materialized source, rather than retaining every Xorb or a complete
/// reconstructed file. Adjacent terms and files sharing that source reuse it.
#[derive(Default)]
pub(super) struct FileDigestVerifier {
    source: Option<(String, MaterializedObject)>,
    #[cfg(test)]
    pub(super) source_reads: usize,
}

impl FileDigestVerifier {
    pub(super) fn retain_verified_source(&mut self, hash: &str, source: MaterializedObject) {
        // Consume each newly validated source before visiting the next one.
        // Retaining every source would make scratch space scale with file size.
        self.source = Some((hash.to_owned(), source));
        #[cfg(test)]
        {
            self.source_reads = self.source_reads.saturating_add(1);
        }
    }

    pub(super) fn release_source(&mut self) {
        self.source = None;
    }

    #[cfg(test)]
    pub(super) fn verify(
        &mut self,
        store: &ServerObjectStore,
        file: &MDBFileInfo,
        expected: Option<&str>,
        derive: bool,
        verified: &HashMap<String, XorbRangeInfo>,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Option<String>, XetAdapterError> {
        let mut digest = FileDigest::new(expected.map(str::to_owned), derive);
        for term in &file.segments {
            let hash = term.xorb_hash.hex();
            let info = verified.get(&hash).ok_or_else(rejected)?;
            digest.update(self, store, term, info, checkpoint)?;
        }
        digest.finish()
    }
}

/// One SHA-256 state per logical file, independent of its source ordering.
pub(super) struct FileDigest {
    expected: Option<String>,
    digest: Option<Sha256>,
}

impl FileDigest {
    pub(super) fn new(expected: Option<String>, derive: bool) -> Self {
        let digest = (expected.is_some() || derive).then(Sha256::new);
        Self { expected, digest }
    }

    pub(super) fn update(
        &mut self,
        verifier: &mut FileDigestVerifier,
        store: &ServerObjectStore,
        term: &FileDataSequenceEntry,
        info: &XorbRangeInfo,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<(), XetAdapterError> {
        let Some(digest) = &mut self.digest else {
            return Ok(());
        };
        checkpoint()?;
        let hash = term.xorb_hash.hex();
        let start = usize::try_from(term.chunk_index_start)?;
        let end = usize::try_from(term.chunk_index_end)?;
        let members = info.verified_chunks.get(start..end).ok_or_else(rejected)?;
        if verifier
            .source
            .as_ref()
            .is_none_or(|(current, _)| *current != hash)
        {
            verifier.source = None;
            // S3 sources spool to bounded-memory temporary storage; local
            // objects are read directly. Do not trust decoded chunk copies.
            let (reader, _) = store
                .materialize_bounded_object_if_present(&xorb_object_key(&hash)?, 0)?
                .ok_or(XetAdapterError::MissingReferencedXorb)?;
            verifier.retain_verified_source(&hash, reader);
        }
        let (_, reader) = verifier.source.as_mut().ok_or_else(rejected)?;
        for member in members {
            checkpoint()?;
            reader.seek(SeekFrom::Start(member.packed_start()))?;
            let (data, packed_length, unpacked_length) = deserialize_chunk(reader).map_err(|error| {
                tracing::debug!(%error, "file SHA-256 verification could not decode Xorb member");
                rejected()
            })?;
            // Authenticate every selected member again, including changes
            // after the complete source validation and before digesting it.
            if Some(u64::try_from(packed_length)?)
                != member.packed_end().checked_sub(member.packed_start())
                || u64::from(unpacked_length) != member.unpacked_len()
                || u64::try_from(data.len())? != member.unpacked_len()
                || compute_data_hash(&data) != MerkleHash::from(*member.hash().as_bytes())
            {
                return Err(rejected());
            }
            digest.update(&data);
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<String>, XetAdapterError> {
        let Some(digest) = self.digest else {
            return Ok(None);
        };
        let actual = format!("{:x}", digest.finalize());
        if self
            .expected
            .as_ref()
            .is_some_and(|expected| actual != *expected)
        {
            return Err(rejected());
        }
        Ok(Some(actual))
    }
}

fn rejected() -> XetAdapterError {
    InvalidSerializedShardError::ParserRejectedMetadata.into()
}
