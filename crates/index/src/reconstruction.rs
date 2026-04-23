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

    /// Returns the Xet xorb referenced by this term.
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
}
