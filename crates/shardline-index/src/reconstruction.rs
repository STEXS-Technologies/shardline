use shardline_protocol::ChunkRange;

use crate::StoredObjectId;

/// A term in a file reconstruction recipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconstructionTerm {
    object_id: StoredObjectId,
    chunk_range: ChunkRange,
    unpacked_length: u64,
}

impl ReconstructionTerm {
    /// Creates a reconstruction term.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_index::{ReconstructionTerm, StoredObjectId};
    /// use shardline_protocol::{ChunkRange, ShardlineHash};
    ///
    /// let object_id = StoredObjectId::new(ShardlineHash::from_bytes([2; 32]));
    /// let term = ReconstructionTerm::new(object_id, ChunkRange::new(0, 2)?, 128);
    /// assert_eq!(term.object_id(), object_id);
    /// assert_eq!(term.chunk_range(), ChunkRange::new(0, 2)?);
    /// assert_eq!(term.unpacked_length(), 128);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub const fn new(
        object_id: StoredObjectId,
        chunk_range: ChunkRange,
        unpacked_length: u64,
    ) -> Self {
        Self {
            object_id,
            chunk_range,
            unpacked_length,
        }
    }

    /// Returns the stored object referenced by this term.
    #[must_use]
    pub const fn object_id(&self) -> StoredObjectId {
        self.object_id
    }

    /// Returns the container object (xorb) referenced by this term.
    #[must_use]
    pub const fn xorb_id(&self) -> StoredObjectId {
        self.object_id
    }

    /// Returns the end-exclusive chunk range referenced by this term.
    #[must_use]
    pub const fn chunk_range(&self) -> ChunkRange {
        self.chunk_range
    }

    /// Returns the unpacked byte length for this term.
    #[must_use]
    pub const fn unpacked_length(&self) -> u64 {
        self.unpacked_length
    }
}

/// File reconstruction recipe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReconstruction {
    terms: Vec<ReconstructionTerm>,
}

impl FileReconstruction {
    /// Creates a file reconstruction from ordered terms.
    #[must_use]
    pub const fn new(terms: Vec<ReconstructionTerm>) -> Self {
        Self { terms }
    }

    /// Returns the ordered reconstruction terms.
    #[must_use]
    pub fn terms(&self) -> &[ReconstructionTerm] {
        &self.terms
    }
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
    use shardline_protocol::{ChunkRange, ShardlineHash};

    use super::{FileReconstruction, ReconstructionTerm};
    use crate::{StoredObjectId, XorbId};

    #[test]
    fn reconstruction_preserves_term_order() {
        let hash = ShardlineHash::from_bytes([7; 32]);
        let xorb_id = XorbId::new(hash);
        let first_range = ChunkRange::new(0, 1);
        let second_range = ChunkRange::new(1, 2);

        assert!(first_range.is_ok());
        assert!(second_range.is_ok());

        let (Ok(first_range), Ok(second_range)) = (first_range, second_range) else {
            return;
        };
        let first = ReconstructionTerm::new(xorb_id, first_range, 64);
        let second = ReconstructionTerm::new(xorb_id, second_range, 128);

        let reconstruction = FileReconstruction::new(vec![first, second]);

        assert_eq!(reconstruction.terms(), &[first, second]);
    }

    #[test]
    fn reconstruction_term_keeps_fields() {
        let hash = ShardlineHash::from_bytes([6; 32]);
        let xorb_id = XorbId::new(hash);
        let range = ChunkRange::new(2, 5);

        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };
        let term = ReconstructionTerm::new(xorb_id, range, 512);

        assert_eq!(term.object_id(), StoredObjectId::new(hash));
        assert_eq!(term.xorb_id(), xorb_id);
        assert_eq!(term.chunk_range(), range);
        assert_eq!(term.unpacked_length(), 512);
    }

    #[test]
    fn reconstruction_term_accessors() {
        let hash = ShardlineHash::from_bytes([9; 32]);
        let xorb_id = XorbId::new(hash);
        let range = ChunkRange::new(0, 10).unwrap();
        let term = ReconstructionTerm::new(xorb_id, range, 1024);

        assert_eq!(term.object_id(), StoredObjectId::new(hash));
        assert_eq!(term.xorb_id(), xorb_id);
        assert_eq!(term.xorb_id(), term.object_id());
        assert_eq!(term.chunk_range(), range);
        assert_eq!(term.chunk_range().start(), 0);
        assert_eq!(term.chunk_range().end_exclusive(), 10);
        assert_eq!(term.unpacked_length(), 1024);
    }

    #[test]
    fn file_reconstruction_new_roundtrips() {
        let hash = ShardlineHash::from_bytes([10; 32]);
        let range = ChunkRange::new(3, 7).unwrap();
        let term = ReconstructionTerm::new(XorbId::new(hash), range, 256);
        let terms = vec![term];
        let reconstruction = FileReconstruction::new(terms);

        assert_eq!(reconstruction.terms().len(), 1);
        assert_eq!(reconstruction.terms()[0], term);
    }

    #[test]
    fn reconstruction_term_debug_format() {
        let hash = ShardlineHash::from_bytes([0; 32]);
        let range = ChunkRange::new(1, 2).unwrap();
        let term = ReconstructionTerm::new(XorbId::new(hash), range, 0);
        let debug = format!("{term:?}");
        assert!(debug.contains("ReconstructionTerm"));
    }
}
