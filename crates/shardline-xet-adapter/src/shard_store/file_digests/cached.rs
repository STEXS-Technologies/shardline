//! Reuse an ordered SHA prefix without widening the authenticated source reads.
use super::*;

impl FileDigest {
    pub(in crate::shard_store) fn update_cached_terms(
        &mut self,
        verifier: &mut FileDigestVerifier,
        store: &ServerObjectStore,
        terms: &[FileDataSequenceEntry],
        info: &XorbRangeInfo,
        checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    ) -> Result<(), XetAdapterError> {
        // Match the prefix grouping used during first-time source validation.
        // Every original length must remain valid even when the prefix hits.
        for term in terms {
            checkpoint()?;
            verification::verify_term_length(term, &info.verified_chunks)?;
        }
        // Prefer a complete ordered prefix only when it already exists.
        // Otherwise retain the existing per-range prefix hits and snapshots,
        // including a reusable first range followed by a changed suffix.
        let ordered_hit = contiguous_source_terms(terms)?.len() != terms.len()
            && self
                .prefixes
                .as_ref()
                .is_some_and(|prefixes| prefixes.contains(terms));
        if ordered_hit {
            self.begin_terms(terms);
        }
        let mut remaining = terms;
        while let Some(first) = remaining.first() {
            let adjacent = contiguous_source_terms(remaining)?;
            if !ordered_hit {
                self.begin_terms(adjacent);
            }
            let mut combined = first.clone();
            for next in adjacent.iter().skip(1) {
                combined.unpacked_segment_bytes = combined
                    .unpacked_segment_bytes
                    .checked_add(next.unpacked_segment_bytes)
                    .ok_or(XetAdapterError::Overflow)?;
                combined.chunk_index_end = next.chunk_index_end;
            }
            // Separate gaps still require separate authenticated reads. A
            // cached SHA state never substitutes for checking today's bytes.
            self.update(verifier, store, &combined, info, checkpoint)?;
            if !ordered_hit {
                self.finish_terms();
            }
            remaining = remaining
                .get(adjacent.len()..)
                .ok_or(XetAdapterError::Overflow)?;
        }
        if ordered_hit {
            self.finish_terms();
        }
        Ok(())
    }
}
