//! Derive and verify file SHA-256 identities from authenticated Xorb members.

use std::io::{Seek, SeekFrom};

use sha2::{Digest, Sha256};
use shardline_server_core::MaterializedObject;
use shardline_xet_core::xorb_object::deserialize_chunk;

use super::*;
mod prefix;
#[cfg(test)]
mod tests;
mod worker;
pub(super) const MIN_PARALLEL_SOURCE_BYTES: u64 = 8 * 1024 * 1024;

/// A forward source-validation pass can digest disjoint, increasing terms in
/// file order. Stop at another source, overlap or a backwards jump; those terms
/// still require independently authenticated reads in logical file order.
pub(super) fn ordered_source_terms(
    terms: &[FileDataSequenceEntry],
) -> Result<&[FileDataSequenceEntry], XetAdapterError> {
    let Some(first) = terms.first() else {
        return Ok(terms);
    };
    let mut previous = first;
    let mut end = 1_usize;
    for next in terms.iter().skip(1) {
        if next.xorb_hash != first.xorb_hash
            || next.chunk_index_start < previous.chunk_index_end
            || next.chunk_index_end <= next.chunk_index_start
        {
            break;
        }
        previous = next;
        end = end.checked_add(1).ok_or(XetAdapterError::Overflow)?;
    }
    terms.get(..end).ok_or(XetAdapterError::Overflow)
}

/// One authenticated range can serve adjacent logical terms without fetching
/// gaps or changing the logical byte order. Repetitions/backwards jumps stay
/// separate, even when they refer to the same immutable container.
pub(super) fn contiguous_source_terms(
    terms: &[FileDataSequenceEntry],
) -> Result<&[FileDataSequenceEntry], XetAdapterError> {
    let Some((first, tail)) = terms.split_first() else {
        return Ok(terms);
    };
    let mut end = first.chunk_index_end;
    let mut count = 1_usize;
    for next in tail {
        if next.xorb_hash != first.xorb_hash
            || next.chunk_index_start != end
            || next.chunk_index_end <= next.chunk_index_start
        {
            break;
        }
        end = next.chunk_index_end;
        count = count.checked_add(1).ok_or(XetAdapterError::Overflow)?;
    }
    terms.get(..count).ok_or(XetAdapterError::Overflow)
}

/// Keep at most one pinned local source, rather than a reconstructed file.
/// Remote sources stream their initial validation and any subsequent ranges.
#[derive(Default)]
pub(super) struct FileDigestVerifier {
    pub(super) parallel_digest: bool,
    source: Option<(String, MaterializedObject)>,
    #[cfg(test)]
    pub(super) source_reads: usize,
    #[cfg(test)]
    pub(super) member_rechecks: usize,
}

impl FileDigestVerifier {
    pub(super) fn retain_verified_source(
        &mut self,
        hash: &str,
        source: Option<MaterializedObject>,
    ) {
        // Consume each newly validated source before visiting the next one.
        // Retaining every source would make scratch space scale with file size.
        self.source = source.map(|source| (hash.to_owned(), source));
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
        checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
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
    prefixes: Option<prefix::Prefixes>,
    reusing_prefix: bool,
    worker: Option<worker::DigestWorker>,
}

impl FileDigest {
    #[cfg(test)]
    pub(super) const fn reuses_verified_prefix(&self) -> bool {
        self.reusing_prefix
    }

    pub(super) fn new(expected: Option<String>, derive: bool) -> Self {
        let digest = (expected.is_some() || derive).then(Sha256::new);
        Self {
            expected,
            digest,
            prefixes: None,
            reusing_prefix: false,
            worker: None,
        }
    }

    pub(super) fn with_scope(expected: Option<String>, scope: Option<&RepositoryScope>) -> Self {
        let mut result = Self::new(expected, scope.is_some());
        result.prefixes = scope.map(prefix::Prefixes::new);
        result
    }

    pub(super) fn begin_terms(&mut self, terms: &[FileDataSequenceEntry]) {
        let cached = self
            .prefixes
            .as_mut()
            .and_then(|prefixes| prefixes.begin(terms));
        self.reusing_prefix = cached.is_some();
        if let Some(cached) = cached {
            if let Some(worker) = &mut self.worker {
                worker.reset(cached.clone());
            }
            self.digest = Some(cached);
        }
    }

    pub(super) fn finish_terms(&mut self) {
        if !self.reusing_prefix
            && let Some(worker) = &mut self.worker
        {
            self.digest = worker.snapshot();
        }
        if !self.reusing_prefix
            && let (Some(prefixes), Some(digest)) = (&mut self.prefixes, &self.digest)
        {
            prefixes.retain(digest);
        }
    }

    pub(super) fn enable_parallel(&mut self, enabled: bool) {
        if enabled && self.digest.is_some() {
            self.worker = worker::DigestWorker::start();
        }
    }

    /// Incorporate only this term's members during full source validation.
    /// This state cannot escape if any later source or file check fails.
    pub(super) fn update_validating_member(
        &mut self,
        term: &FileDataSequenceEntry,
        member: u64,
        data: &[u8],
    ) {
        if member >= term.chunk_index_start
            && member < term.chunk_index_end
            && !self.reusing_prefix
            && let Some(digest) = &mut self.digest
        {
            if let Some(worker) = &mut self.worker {
                worker.update(data);
            } else {
                digest.update(data);
            }
        }
    }

    /// Advance through a disjoint ordered run while the source validator visits
    /// members. Unselected gaps never enter the file digest. No chunk hash is
    /// substituted for SHA-256 of the selected decoded bytes.
    pub(super) fn update_validating_terms(
        &mut self,
        terms: &mut &[FileDataSequenceEntry],
        member: u64,
        data: &[u8],
    ) {
        while let Some((term, tail)) = terms.split_first() {
            if member < term.chunk_index_end {
                self.update_validating_member(term, member, data);
                break;
            }
            *terms = tail;
        }
    }

    pub(super) fn update(
        &mut self,
        verifier: &mut FileDigestVerifier,
        store: &ServerObjectStore,
        term: &FileDataSequenceEntry,
        info: &XorbRangeInfo,
        checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    ) -> Result<(), XetAdapterError> {
        let reusing_prefix = self.reusing_prefix;
        let mut worker = self.worker.as_mut();
        let Some(digest) = &mut self.digest else {
            return Ok(());
        };
        checkpoint()?;
        let hash = term.xorb_hash.hex();
        let start = usize::try_from(term.chunk_index_start)?;
        let end = usize::try_from(term.chunk_index_end)?;
        let members = info.verified_chunks.get(start..end).ok_or_else(rejected)?;
        if let ServerObjectStore::S3(remote) = store {
            streaming_source::visit_range(
                remote,
                &xorb_object_key(&hash)?,
                members,
                checkpoint,
                &mut |_, data| {
                    if !reusing_prefix {
                        if let Some(worker) = &mut worker {
                            worker.update(data);
                        } else {
                            digest.update(data);
                        }
                    }
                    Ok(())
                },
            )?;
            #[cfg(test)]
            {
                verifier.source_reads = verifier.source_reads.saturating_add(1);
                verifier.member_rechecks = verifier.member_rechecks.saturating_add(members.len());
            }
            return Ok(());
        }
        if verifier
            .source
            .as_ref()
            .is_none_or(|(current, _)| *current != hash)
        {
            verifier.source = None;
            // Remote ranges returned above. Pin the local immutable source;
            // do not trust separately stored decoded chunk copies.
            let (reader, _) = store
                .materialize_bounded_object_if_present_with_checkpoint(
                    &xorb_object_key(&hash)?,
                    0,
                    checkpoint,
                )?
                .ok_or(XetAdapterError::MissingReferencedXorb)?;
            verifier.retain_verified_source(&hash, Some(reader));
        }
        let (_, reader) = verifier.source.as_mut().ok_or_else(rejected)?;
        for member in members {
            checkpoint()?;
            #[cfg(test)]
            {
                verifier.member_rechecks = verifier.member_rechecks.saturating_add(1);
            }
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
            if !reusing_prefix {
                if let Some(worker) = &mut worker {
                    worker.update(&data);
                } else {
                    digest.update(&data);
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<String>, XetAdapterError> {
        let digest = match self.worker {
            Some(worker) => Some(worker.finish().ok_or_else(|| {
                XetAdapterError::Io(std::io::Error::other("file SHA-256 worker failed"))
            })?),
            None => self.digest,
        };
        let Some(digest) = digest else {
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
        if let Some(prefixes) = self.prefixes {
            prefixes.publish();
        }
        Ok(Some(actual))
    }
}

fn rejected() -> XetAdapterError {
    InvalidSerializedShardError::ParserRejectedMetadata.into()
}
