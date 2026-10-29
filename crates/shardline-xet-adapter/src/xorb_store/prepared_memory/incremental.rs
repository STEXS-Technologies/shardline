//! Authenticate complete members as transport frames arrive. No publication
//! result exists until EOF, footer/root checks and the serialized commitment.
use super::{PreparedXorbMemory, XetAdapterError, parse_xet_hash_hex};
use shardline_storage::VerifiedBodyBuilder;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        XORB_CHUNK_HEADER_LENGTH, XorbObject, XorbObjectInfoV1, deserialize_chunk,
        parse_chunk_header,
    },
};
use std::io::Cursor;

/// Request-owned incremental native verification. Call only under aggregate
/// memory and CPU admission; this type bounds serialized bytes, not concurrency.
pub struct IncrementalXorbMemory {
    expected_hash: String,
    maximum: usize,
    bytes: VerifiedBodyBuilder,
    position: usize,
    info: XorbObjectInfoV1,
    footer: bool,
    full_validation: bool,
}

impl IncrementalXorbMemory {
    /// Start a bounded container. Invalid roots fail before receiving payload.
    ///
    /// # Errors
    /// Returns invalid content identity errors.
    pub fn new(expected_hash: &str, maximum: usize) -> Result<Self, XetAdapterError> {
        parse_xet_hash_hex(expected_hash)?;
        Ok(Self {
            expected_hash: expected_hash.to_owned(),
            maximum,
            bytes: VerifiedBodyBuilder::default(),
            position: 0,
            info: XorbObjectInfoV1::default(),
            footer: false,
            full_validation: false,
        })
    }

    /// Number of members already decoded and hashed, before EOF/root binding.
    #[must_use]
    pub const fn verified_member_count(&self) -> usize {
        self.info.chunk_hashes.len()
    }

    /// Consume a transport frame and authenticate every newly complete member.
    /// An error consumes the state, preventing publication of a valid prefix.
    ///
    /// # Errors
    /// Rejects malformed members, decompression/size excess and cancellation.
    pub fn push(
        mut self,
        frame: &[u8],
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<Self, XetAdapterError> {
        checkpoint()?;
        let length = self
            .bytes
            .len()
            .checked_add(frame.len())
            .ok_or(XetAdapterError::Overflow)?;
        if length > self.maximum {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        if length > self.bytes.capacity() {
            let capacity = length
                .checked_next_power_of_two()
                .unwrap_or(self.maximum)
                .min(self.maximum);
            self.bytes
                .try_reserve_exact(
                    capacity
                        .checked_sub(self.bytes.len())
                        .ok_or(XetAdapterError::Overflow)?,
                )
                .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
        }
        self.bytes.extend_from_slice(frame);
        while !self.footer
            && !self.full_validation
            && self
                .bytes
                .len()
                .checked_sub(self.position)
                .ok_or(XetAdapterError::Overflow)?
                >= XORB_CHUNK_HEADER_LENGTH
        {
            checkpoint()?;
            let header = match parse_chunk_header(
                self.bytes
                    .get(
                        self.position
                            ..self
                                .position
                                .checked_add(XORB_CHUNK_HEADER_LENGTH)
                                .ok_or(XetAdapterError::Overflow)?,
                    )
                    .ok_or(XetAdapterError::InvalidSerializedXorb)?
                    .try_into()
                    .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?,
            ) {
                Ok(header) => header,
                Err(shardline_xet_core::error::CoreError::ChunkHeaderParse) => {
                    self.footer = true;
                    break;
                }
                Err(error) => return Err(super::super::normalization::normalization_error(error)),
            };
            if self.info.chunk_hashes.len() >= 8192 {
                // Footerless normalization has this limit, but existing
                // canonical containers can have larger valid member tables.
                // Bound our incremental table without narrowing that contract.
                self.full_validation = true;
                break;
            }
            let unpacked = self
                .info
                .unpacked_chunk_offsets
                .last()
                .copied()
                .unwrap_or(0)
                .checked_add(u64::from(header.get_uncompressed_length()))
                .ok_or(XetAdapterError::Overflow)?;
            if unpacked > crate::xorb::MAX_XORB_UNPACKED_BYTES {
                return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
            }
            if unpacked > 64 * 1024 * 1024 {
                // The wire form is not known until EOF. Footerless input must
                // keep its existing 64 MiB normalization ceiling; unusually
                // large canonical input keeps full-validation compatibility.
                // Do not decode up to the canonical 4 GiB ceiling only to
                // discover an oversized footerless request at EOF.
                self.full_validation = true;
                break;
            }
            let end = self
                .position
                .checked_add(XORB_CHUNK_HEADER_LENGTH)
                .and_then(|start| start.checked_add(header.get_compressed_length() as usize))
                .ok_or(XetAdapterError::Overflow)?;
            if end > self.bytes.len() {
                break;
            }
            let (decoded, consumed, _) = deserialize_chunk(&mut Cursor::new(
                self.bytes
                    .get(self.position..end)
                    .ok_or(XetAdapterError::InvalidSerializedXorb)?,
            ))
            .map_err(super::super::normalization::normalization_error)?;
            if consumed
                != end
                    .checked_sub(self.position)
                    .ok_or(XetAdapterError::Overflow)?
            {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            self.info.chunk_hashes.push(compute_data_hash(&decoded));
            self.info.chunk_boundary_offsets.push(u64::try_from(end)?);
            self.info.unpacked_chunk_offsets.push(unpacked);
            self.position = end;
        }
        Ok(self)
    }

    /// Bind verified members to the requested root and supplied/generated footer.
    /// Serialized bytes remain private and immutable through publication.
    ///
    /// # Errors
    /// Rejects incomplete input, forged footer/root, oversized normalization or cancellation.
    pub fn finish(
        mut self,
        checkpoint: &dyn Fn() -> std::io::Result<()>,
    ) -> Result<PreparedXorbMemory, XetAdapterError> {
        checkpoint()?;
        if self.full_validation {
            return super::prepare_owned_xorb_memory_with_checkpoint(
                &self.expected_hash,
                self.bytes.into_vec(),
                checkpoint,
            );
        }
        if self.info.chunk_hashes.is_empty() {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        let mut previous = 0;
        let leaves: Vec<_> = self
            .info
            .chunk_hashes
            .iter()
            .copied()
            .zip(self.info.unpacked_chunk_offsets.iter().map(|end| {
                let length = end.saturating_sub(previous);
                previous = *end;
                length
            }))
            .collect();
        self.info.xorb_hash = xorb_hash(&leaves);
        if parse_xet_hash_hex(&self.info.xorb_hash.hex())?
            != parse_xet_hash_hex(&self.expected_hash)?
        {
            return Err(XetAdapterError::XorbHashMismatch);
        }
        self.info.num_chunks = u64::try_from(leaves.len())?;
        self.info.fill_in_boundary_offsets();
        let canonical = if self.footer {
            let parsed = XorbObject::deserialize(&mut Cursor::new(&self.bytes))
                .map_err(super::super::normalization::normalization_error)?;
            if parsed.info.boundaries_version != 1 {
                // Historical footers need the established legacy interpretation.
                return super::prepare_owned_xorb_memory_with_checkpoint(
                    &self.expected_hash,
                    self.bytes.into_vec(),
                    checkpoint,
                );
            }
            if parsed.info.xorb_hash != self.info.xorb_hash
                || parsed.info.num_chunks != self.info.num_chunks
                || parsed.info.chunk_hashes != self.info.chunk_hashes
                || parsed.info.chunk_boundary_offsets != self.info.chunk_boundary_offsets
                || parsed.info.unpacked_chunk_offsets != self.info.unpacked_chunk_offsets
                || parsed
                    .get_contents_length()
                    .map_err(super::super::normalization::normalization_error)?
                    != u64::try_from(self.position)?
            {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            parsed
        } else {
            if self.position != self.bytes.len() {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            if previous > 64 * 1024 * 1024 {
                return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
            }
            let footer_length = self
                .info
                .serialized_length()
                .checked_add(8)
                .ok_or(XetAdapterError::Overflow)?;
            self.bytes
                .try_reserve_exact(footer_length)
                .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
            XorbObject::serialize_given_info(&mut self.bytes, self.info)
                .map_err(super::super::normalization::normalization_error)?
                .0
        };
        checkpoint()?;
        let expected = parse_xet_hash_hex(&self.expected_hash)?;
        let length = u64::try_from(self.bytes.len())?;
        let validated = crate::xorb::metadata_from_verified_xorb(expected, length, &canonical)?;
        // Seal exactly the bytes accumulated and authenticated by storage's
        // builder, including any generated footer. No mutable bytes escape.
        let bytes = self.bytes.finish();
        let integrity = bytes.integrity();
        checkpoint()?;
        Ok(PreparedXorbMemory {
            expected_hash: self.expected_hash,
            bytes,
            validated,
            integrity,
        })
    }
}
