//! Parsing and decoding of serialized Xorb byte ranges (M2a).
//!
//! The Xet download path fetches a **byte range** of a serialized xorb (the
//! chunk payload, excluding the footer), so a reader must be able to walk the
//! chunk stream without any footer metadata. This module is therefore
//! **footer-less tolerant** (`docs/SDX_PLAN.md` §7.8): it decodes the chunk
//! stream sequentially via the pinned upstream `xet-core-structures` public
//! API [`deserialize_chunks`](xet_core_structures::xorb_object::deserialize_chunks)
//! — the same call the reference client uses for ranged xorb fetches
//! (`xet-client-1.5.4/src/cas_client/remote_client.rs:455`) — and exposes each
//! decoded chunk with its data hash (BLAKE3 `DATA_KEY`, M0
//! [`compute_chunk_hash`](crate::hash::compute_chunk_hash)).
//!
//! ByteGrouping4LZ4 / chunk deserialization is **not reimplemented**; the
//! pinned `xet-core-structures` crate owns the format (plan §4.1 / §11 Q3).

use std::io::Cursor;

use thiserror::Error;
use xet_core_structures::error::CoreError;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::xorb_object::deserialize_chunks;

use crate::hash::compute_chunk_hash;

/// Failure to decode a serialized xorb byte range.
#[derive(Debug, Error)]
pub enum XorbError {
    /// The serialized chunk stream was malformed or corrupted.
    #[error("failed to parse serialized xorb: {0}")]
    Format(#[from] CoreError),
    /// A decoded chunk's byte offsets were inconsistent with the data.
    #[error("xorb chunk byte offsets are inconsistent with the decoded data")]
    InconsistentChunkOffsets,
}

/// A single decoded chunk from a serialized xorb byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedChunk {
    /// Decompressed chunk data.
    pub data: Vec<u8>,
    /// Data hash of the chunk: `compute_chunk_hash(data)` (BLAKE3 `DATA_KEY`).
    pub hash: MerkleHash,
}

/// Reader over serialized xorb bytes.
///
/// Constructed from a fetched xorb byte range (see
/// [`crate::transfer::TransferClient::fetch_xorb_range`]). The bytes are the
/// concatenated serialized chunk payload; the footer is not required.
#[derive(Debug, Clone)]
pub struct XorbReader {
    bytes: Vec<u8>,
}

impl XorbReader {
    /// Creates a reader over the serialized chunk payload `bytes`.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Decodes every chunk in the serialized payload, in order.
    ///
    /// Each chunk is decompressed and its data hash computed. Chunk headers are
    /// validated by the upstream decoder (compression scheme, length bounds,
    /// and the uncompressed-length-vs-header agreement), so a corrupted stream
    /// surfaces as [`XorbError::Format`].
    ///
    /// # Errors
    ///
    /// Returns [`XorbError::Format`] when the chunk stream is malformed or a
    /// chunk fails to decompress, and [`XorbError::InconsistentChunkOffsets`]
    /// when the decoder's chunk offsets disagree with the decoded data length.
    pub fn decode_chunks(&self) -> Result<Vec<DecodedChunk>, XorbError> {
        let (data, offsets) = deserialize_chunks(&mut Cursor::new(self.bytes.as_slice()))?;
        let mut chunks = Vec::with_capacity(offsets.len().saturating_sub(1));
        let mut previous_offset: Option<u32> = None;
        for offset in offsets {
            if let Some(start) = previous_offset {
                let end = offset;
                let chunk_bytes = data
                    .get(start as usize..end as usize)
                    .ok_or(XorbError::InconsistentChunkOffsets)?;
                let hash = compute_chunk_hash(chunk_bytes);
                chunks.push(DecodedChunk {
                    data: chunk_bytes.to_vec(),
                    hash,
                });
            }
            previous_offset = Some(offset);
        }
        Ok(chunks)
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;
    use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};

    use super::{DecodedChunk, XorbError, XorbReader};
    use crate::hash::compute_chunk_hash;

    /// Serializes the given chunk payloads (without a footer) using the
    /// pinned upstream chunk serializer.
    fn serialize_payload(chunks: &[&[u8]]) -> Vec<u8> {
        let mut payload = Vec::new();
        for chunk in chunks {
            serialize_chunk(chunk, &mut payload, CompressionScheme::None).unwrap();
        }
        payload
    }

    fn expected_chunks(chunks: &[&[u8]]) -> Vec<DecodedChunk> {
        chunks
            .iter()
            .map(|data| DecodedChunk {
                data: data.to_vec(),
                hash: compute_chunk_hash(data),
            })
            .collect()
    }

    #[test]
    fn decode_chunks_round_trips_payload_without_footer() {
        let chunk_a = b"hello xet chunk";
        let chunk_b = b"second chunk payload";
        let payload = serialize_payload(&[chunk_a, chunk_b]);

        let decoded = XorbReader::new(payload).decode_chunks().unwrap();

        assert_eq!(decoded, expected_chunks(&[chunk_a, chunk_b]));
    }

    #[test]
    fn decode_chunks_single_chunk() {
        let chunk = b"just one chunk";
        let payload = serialize_payload(&[chunk]);

        let decoded = XorbReader::new(payload).decode_chunks().unwrap();

        assert_eq!(decoded, expected_chunks(&[chunk]));
    }

    #[test]
    fn decode_chunks_empty_payload_yields_no_chunks() {
        let decoded = XorbReader::new(Vec::new()).decode_chunks().unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_chunks_rejects_truncated_chunk() {
        let chunk = b"truncated-chunk-data";
        let mut payload = serialize_payload(&[chunk]);
        payload.truncate(payload.len() - 3);

        let result = XorbReader::new(payload).decode_chunks();
        assert!(matches!(result, Err(XorbError::Format(_))));
    }

    #[test]
    fn decode_chunks_rejects_footer_appended_after_payload() {
        // A full serialized xorb (payload + footer) must NOT be fed to the
        // chunk-stream decoder: the footer's "XETBLOB" ident is rejected by
        // the chunk header parser. Footer parsing is XorbObject::deserialize's
        // job; the transfer endpoint serves payload-only ranges.
        let chunk = b"with-footer";
        let mut payload = serialize_payload(&[chunk]);
        payload.extend_from_slice(&[b'X', b'E', b'T', b'B', b'L', b'O', b'B', 1, 0, 0, 0]);

        let result = XorbReader::new(payload).decode_chunks();
        assert!(matches!(result, Err(XorbError::Format(_))));
    }

    #[test]
    fn decode_chunks_computes_data_hashes() {
        let chunk = b"hash me";
        let payload = serialize_payload(&[chunk]);
        let decoded = XorbReader::new(payload).decode_chunks().unwrap();
        assert_eq!(decoded[0].hash, compute_chunk_hash(chunk));
        assert_ne!(decoded[0].hash, compute_chunk_hash(b"hash different"));
    }

    #[test]
    fn decode_chunks_handles_incompressible_data() {
        // CompressionScheme::None keeps data byte-identical even for highly
        // compressible input; use a varied payload to exercise the path.
        let chunk: Vec<u8> = (0..512).map(|i| (i * 31 % 251) as u8).collect();
        let payload = serialize_payload(&[&chunk]);
        let decoded = XorbReader::new(payload).decode_chunks().unwrap();
        assert_eq!(decoded[0].data, chunk);
    }

    #[test]
    fn term_verification_hash_matches_xorb_footer_chunk_range_hash() {
        use std::io::Cursor;

        use xet_core_structures::merklehash::xorb_hash;
        use xet_core_structures::xorb_object::{XorbObject, XorbObjectInfoV1, serialize_chunk};

        // Build a full serialized xorb (payload + footer) using the pinned
        // upstream public API, then confirm that the term verification hash
        // computed from the *decoded* chunk hashes equals the xorb footer's
        // authoritative `generate_chunk_range_hash` (range_hash_from_chunks /
        // VERIFICATION_KEY) over its stored chunk table.
        let chunks: [Vec<u8>; 3] = [
            b"first chunk of the xorb".to_vec(),
            b"second chunk of the xorb".to_vec(),
            b"third chunk of the xorb".to_vec(),
        ];
        let chunk_hashes: Vec<MerkleHash> = chunks
            .iter()
            .map(|chunk| compute_chunk_hash(chunk))
            .collect();

        let mut payload = Vec::new();
        let mut boundaries: Vec<u32> = Vec::new();
        let mut unpacked: Vec<u32> = Vec::new();
        let mut packed_total: u32 = 0;
        let mut unpacked_total: u32 = 0;
        for chunk in &chunks {
            let written = serialize_chunk(chunk, &mut payload, CompressionScheme::None).unwrap();
            packed_total = packed_total.saturating_add(u32::try_from(written).unwrap());
            boundaries.push(packed_total);
            unpacked_total = unpacked_total.saturating_add(u32::try_from(chunk.len()).unwrap());
            unpacked.push(unpacked_total);
        }

        let mut info = XorbObjectInfoV1::default();
        info.xorb_hash = xorb_hash(
            &chunks
                .iter()
                .map(|chunk| {
                    (
                        compute_chunk_hash(chunk),
                        u64::try_from(chunk.len()).unwrap(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        info.num_chunks = u32::try_from(chunks.len()).unwrap();
        info.chunk_hashes = chunk_hashes.clone();
        info.chunk_boundary_offsets = boundaries;
        info.unpacked_chunk_offsets = unpacked;
        info.fill_in_boundary_offsets();
        // Content payload length excludes the footer.
        let content_len = usize::try_from(packed_total).unwrap();
        let (_xorb_object, _footer_len) =
            XorbObject::serialize_given_info(&mut payload, info).unwrap();

        // Decode the content payload (footer excluded).
        let decoded = XorbReader::new(payload[..content_len].to_vec())
            .decode_chunks()
            .unwrap();
        let decoded_hashes: Vec<MerkleHash> = decoded.iter().map(|chunk| chunk.hash).collect();
        let computed = crate::hash::compute_term_verification_hash(&decoded_hashes);

        // The footer's authoritative chunk-range hash over the same chunk range.
        let xorb = XorbObject::deserialize(&mut Cursor::new(payload.as_slice())).unwrap();
        let authoritative = xorb
            .generate_chunk_range_hash(0, u32::try_from(chunks.len()).unwrap())
            .unwrap();

        assert_eq!(computed, authoritative);
        assert_eq!(decoded_hashes, chunk_hashes);
    }
}
