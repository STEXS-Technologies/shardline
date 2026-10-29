//! Forward-only verification: retain one encoded member and footer metadata,
//! never a complete container. Visitors remain provisional until finish.
use super::*;
use shardline_xet_core::{
    merklehash::xorb_hash,
    xorb_object::{XORB_CHUNK_HEADER_LENGTH, XorbObjectInfoV1, parse_chunk_header},
};

#[cfg(test)]
mod tests;

pub(crate) struct StreamingXorb<'request> {
    pending: Vec<u8>,
    info: XorbObjectInfoV1,
    packed: u64,
    footer: bool,
    checkpoint: &'request (dyn Fn() -> std::io::Result<()> + Sync),
    visitor: &'request mut (dyn FnMut(u64, &[u8]) -> std::io::Result<()> + Send),
    failure: Option<XetAdapterError>,
}

impl<'request> StreamingXorb<'request> {
    pub(crate) fn new(
        checkpoint: &'request (dyn Fn() -> std::io::Result<()> + Sync),
        visitor: &'request mut (dyn FnMut(u64, &[u8]) -> std::io::Result<()> + Send),
    ) -> Self {
        Self {
            pending: Vec::new(),
            info: XorbObjectInfoV1::default(),
            packed: 0,
            footer: false,
            checkpoint,
            visitor,
            failure: None,
        }
    }

    pub(crate) const fn take_failure(&mut self) -> Option<XetAdapterError> {
        if self.failure.is_some() {
            self.failure.replace(XetAdapterError::InvalidSerializedXorb)
        } else {
            None
        }
    }

    fn append(&mut self, bytes: &[u8], limit: usize) -> Result<(), XetAdapterError> {
        let length = self
            .pending
            .len()
            .checked_add(bytes.len())
            .ok_or(XetAdapterError::Overflow)?;
        if length > limit {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        if self.pending.capacity() < length {
            self.pending
                .try_reserve_exact(
                    limit
                        .checked_sub(self.pending.len())
                        .ok_or(XetAdapterError::Overflow)?,
                )
                .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
        }
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    fn push(&mut self, mut bytes: &[u8]) -> Result<(), XetAdapterError> {
        while !bytes.is_empty() {
            (self.checkpoint)()?;
            if self.footer {
                let limit = self
                    .info
                    .serialized_length()
                    .checked_add(8)
                    .ok_or(XetAdapterError::Overflow)?;
                return self.append(bytes, limit);
            }
            if self.pending.len() < XORB_CHUNK_HEADER_LENGTH {
                let take = XORB_CHUNK_HEADER_LENGTH
                    .checked_sub(self.pending.len())
                    .ok_or(XetAdapterError::Overflow)?
                    .min(bytes.len());
                let (head, tail) = bytes.split_at(take);
                self.append(head, XORB_CHUNK_HEADER_LENGTH)?;
                bytes = tail;
                if self.pending.len() < XORB_CHUNK_HEADER_LENGTH {
                    continue;
                }
            }
            let header = match parse_chunk_header(
                self.pending
                    .get(..XORB_CHUNK_HEADER_LENGTH)
                    .ok_or(XetAdapterError::InvalidSerializedXorb)?
                    .try_into()
                    .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?,
            ) {
                Ok(header) => header,
                Err(CoreError::ChunkHeaderParse) => {
                    self.footer = true;
                    self.pending = self.pending.as_slice().to_vec();
                    continue;
                }
                Err(error) => return Err(map_core_error(&error).into()),
            };
            let unpacked = self
                .info
                .unpacked_chunk_offsets
                .last()
                .copied()
                .unwrap_or(0)
                .checked_add(u64::from(header.get_uncompressed_length()))
                .ok_or(XetAdapterError::Overflow)?;
            if unpacked > MAX_XORB_UNPACKED_BYTES {
                return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
            }
            if header.get_uncompressed_length() == 0 {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            let length = XORB_CHUNK_HEADER_LENGTH
                .checked_add(usize::try_from(header.get_compressed_length())?)
                .ok_or(XetAdapterError::Overflow)?;
            let take = length
                .checked_sub(self.pending.len())
                .ok_or(XetAdapterError::Overflow)?
                .min(bytes.len());
            let (head, tail) = bytes.split_at(take);
            self.append(head, length)?;
            bytes = tail;
            if self.pending.len() != length {
                continue;
            }
            let (decoded, consumed, actual) = deserialize_chunk(&mut Cursor::new(&self.pending))
                .map_err(|error| map_core_error(&error))?;
            if consumed != length || actual != header.get_uncompressed_length() {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            let member = u64::try_from(self.info.chunk_hashes.len())?;
            self.info.chunk_hashes.push(compute_data_hash(&decoded));
            self.packed = self
                .packed
                .checked_add(u64::try_from(length)?)
                .ok_or(XetAdapterError::Overflow)?;
            self.info.chunk_boundary_offsets.push(self.packed);
            self.info.unpacked_chunk_offsets.push(unpacked);
            (self.checkpoint)()?;
            (self.visitor)(member, &decoded)?;
            self.pending.clear();
        }
        Ok(())
    }

    pub(crate) fn finish_full(
        self,
        expected: ShardlineHash,
        length: u64,
    ) -> Result<ValidatedXorb, XetAdapterError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        (self.checkpoint)()?;
        if !self.footer
            || self.info.chunk_hashes.is_empty()
            || self.packed.checked_add(u64::try_from(self.pending.len())?) != Some(length)
        {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        let mut parsed = XorbObject::deserialize(&mut Cursor::new(&self.pending))
            .map_err(|error| map_core_error(&error))?;
        if parsed.info_length.checked_add(8) != Some(u64::try_from(self.pending.len())?)
            || parsed.info.num_chunks != u64::try_from(self.info.chunk_hashes.len())?
            || parsed.info.chunk_hashes != self.info.chunk_hashes
            || parsed.info.chunk_boundary_offsets != self.info.chunk_boundary_offsets
            || (parsed.info.boundaries_version == 1
                && parsed.info.unpacked_chunk_offsets != self.info.unpacked_chunk_offsets)
        {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        let mut previous = 0;
        let leaves: Vec<_> = self
            .info
            .chunk_hashes
            .iter()
            .copied()
            .zip(self.info.unpacked_chunk_offsets.iter().map(|end| {
                let member_length = end.saturating_sub(previous);
                previous = *end;
                member_length
            }))
            .collect();
        let actual = xorb_hash(&leaves);
        if actual != parsed.info.xorb_hash || merkle_hash_to_shardline_hash(actual) != expected {
            return Err(XetAdapterError::XorbHashMismatch);
        }
        parsed.info.unpacked_chunk_offsets = self.info.unpacked_chunk_offsets;
        parsed.info.boundaries_version = 1;
        metadata_from_verified_xorb(expected, length, &parsed).map_err(Into::into)
    }

    pub(crate) fn finish_range(
        self,
        members: &[ValidatedXorbChunk],
    ) -> Result<(), XetAdapterError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        (self.checkpoint)()?;
        if self.footer || !self.pending.is_empty() || members.len() != self.info.chunk_hashes.len()
        {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        let base = members
            .first()
            .ok_or(XetAdapterError::InvalidSerializedXorb)?
            .packed_start();
        let unpacked_base = members
            .first()
            .ok_or(XetAdapterError::InvalidSerializedXorb)?
            .unpacked_start();
        for (((member, hash), packed), unpacked) in members
            .iter()
            .zip(self.info.chunk_hashes)
            .zip(self.info.chunk_boundary_offsets)
            .zip(self.info.unpacked_chunk_offsets)
        {
            if member.hash() != merkle_hash_to_shardline_hash(hash)
                || member.packed_end().checked_sub(base) != Some(packed)
                || member.unpacked_end().checked_sub(unpacked_base) != Some(unpacked)
            {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
        }
        Ok(())
    }
}

impl std::io::Write for StreamingXorb<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.failure.is_some() {
            return Err(std::io::Error::other("Xorb verification already failed"));
        }
        match self.push(bytes) {
            Ok(()) => Ok(bytes.len()),
            Err(error) => {
                self.failure = Some(error);
                // Never return Interrupted: write_all would retry cancellation.
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Xorb verification failed",
                ))
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
