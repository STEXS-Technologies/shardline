//! Rebuildable metadata derived from fully validated native Xorb content.

use serde::{Deserialize, Serialize};

/// Maximum members in a standard Xet Xorb's bounded lookup descriptor.
pub const MAX_XET_DESCRIPTOR_CHUNKS: usize = 8192;

pub(crate) fn member_rows_match(descriptor: &XetXorbDescriptor, rows: &[(i64, String)]) -> bool {
    rows.len() == descriptor.chunks.len()
        && rows.iter().zip(&descriptor.chunks).enumerate().all(
            |(index, ((stored_index, hash), chunk))| {
                i64::try_from(index).ok() == Some(*stored_index) && *hash == chunk.hash
            },
        )
}

/// One verified member's content identity and serialized/decoded boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XetXorbChunk {
    /// Native Xet chunk hash, in wire path order.
    pub hash: String,
    /// Inclusive serialized start.
    pub packed_start: u64,
    /// Exclusive serialized end.
    pub packed_end: u64,
    /// Inclusive decoded start.
    pub unpacked_start: u64,
    /// Exclusive decoded end.
    pub unpacked_end: u64,
}

/// Complete immutable descriptor, published only after content verification.
///
/// This is derived metadata, not an authorization grant or an existence proof.
/// Consumers must verify the native root and authoritative record membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XetXorbDescriptor {
    /// Native Xorb hash, in wire path order.
    pub hash: String,
    /// Complete serialized object length, including footer.
    pub total_bytes: u64,
    /// Every chunk, in original container order.
    pub chunks: Vec<XetXorbChunk>,
}

impl XetXorbDescriptor {
    /// Checks bounded, monotonic descriptor structure. This does not read or
    /// authenticate the source container; the publisher must do that first.
    #[must_use]
    pub fn has_valid_structure(&self) -> bool {
        if self.chunks.is_empty()
            || self.chunks.len() > MAX_XET_DESCRIPTOR_CHUNKS
            || crate::parse_xet_hash_hex(&self.hash).is_err()
            || self.total_bytes > i64::MAX as u64
        {
            return false;
        }
        let mut packed = 0;
        let mut unpacked = 0;
        for chunk in &self.chunks {
            if crate::parse_xet_hash_hex(&chunk.hash).is_err()
                || chunk.packed_start != packed
                || chunk.unpacked_start != unpacked
                || chunk.packed_end <= packed
                || chunk.unpacked_end <= unpacked
                || chunk.unpacked_end > i64::MAX as u64
            {
                return false;
            }
            packed = chunk.packed_end;
            unpacked = chunk.unpacked_end;
        }
        packed < self.total_bytes
    }
}

#[cfg(test)]
mod tests;
