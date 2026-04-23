use std::{
    error::Error as StdError,
    io::{Error as IoError, Read, Seek, SeekFrom},
    num::TryFromIntError,
};

use thiserror::Error;
use xet_core_structures::{
    error::CoreError,
    merklehash::{MerkleHash, compute_data_hash},
    xorb_object::{XorbObject, deserialize_chunk},
};

use crate::ShardlineHash;

/// Validated metadata for one chunk inside a serialized xorb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedXorbChunk {
    hash: ShardlineHash,
    packed_start: u64,
    packed_end: u64,
    unpacked_start: u64,
    unpacked_end: u64,
}

impl ValidatedXorbChunk {
    /// Creates validated chunk metadata.
    #[must_use]
    pub const fn new(
        hash: ShardlineHash,
        packed_start: u64,
        packed_end: u64,
        unpacked_start: u64,
        unpacked_end: u64,
    ) -> Self {
        Self {
            hash,
            packed_start,
            packed_end,
            unpacked_start,
            unpacked_end,
        }
    }

    /// Returns the chunk content hash.
    #[must_use]
    pub const fn hash(&self) -> ShardlineHash {
        self.hash
    }

    /// Returns the byte offset where this packed chunk starts in the serialized xorb.
    #[must_use]
    pub const fn packed_start(&self) -> u64 {
        self.packed_start
    }

    /// Returns the byte offset where this packed chunk ends in the serialized xorb.
    #[must_use]
    pub const fn packed_end(&self) -> u64 {
        self.packed_end
    }

    /// Returns the unpacked file offset where this chunk starts.
    #[must_use]
    pub const fn unpacked_start(&self) -> u64 {
        self.unpacked_start
    }

    /// Returns the unpacked file offset where this chunk ends.
    #[must_use]
    pub const fn unpacked_end(&self) -> u64 {
        self.unpacked_end
    }

    /// Returns the unpacked byte length represented by this chunk.
    #[must_use]
    pub const fn unpacked_len(&self) -> u64 {
        self.unpacked_end.saturating_sub(self.unpacked_start)
    }
}

/// Validated top-level metadata for a serialized xorb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedXorb {
    hash: ShardlineHash,
    total_length: u64,
    packed_content_length: u64,
    unpacked_length: u64,
    chunks: Vec<ValidatedXorbChunk>,
}

impl ValidatedXorb {
    /// Creates validated xorb metadata.
    #[must_use]
    pub const fn new(
        hash: ShardlineHash,
        total_length: u64,
        packed_content_length: u64,
        unpacked_length: u64,
        chunks: Vec<ValidatedXorbChunk>,
    ) -> Self {
        Self {
            hash,
            total_length,
            packed_content_length,
            unpacked_length,
            chunks,
        }
    }

    /// Returns the xorb content hash.
    #[must_use]
    pub const fn hash(&self) -> ShardlineHash {
        self.hash
    }

    /// Returns the full serialized xorb length in bytes.
    #[must_use]
    pub const fn total_length(&self) -> u64 {
        self.total_length
    }

    /// Returns the packed content length reported by the xorb footer.
    #[must_use]
    pub const fn packed_content_length(&self) -> u64 {
        self.packed_content_length
    }

    /// Returns the total unpacked byte length represented by all chunks.
    #[must_use]
    pub const fn unpacked_length(&self) -> u64 {
        self.unpacked_length
    }

    /// Returns validated chunks in xorb order.
    #[must_use]
    pub fn chunks(&self) -> &[ValidatedXorbChunk] {
        &self.chunks
    }
}

/// Decoded chunk payload paired with its validated descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedXorbChunk {
    descriptor: ValidatedXorbChunk,
    data: Vec<u8>,
}

impl DecodedXorbChunk {
    /// Creates a decoded chunk from validated metadata and payload bytes.
    #[must_use]
    pub const fn new(descriptor: ValidatedXorbChunk, data: Vec<u8>) -> Self {
        Self { descriptor, data }
    }

    /// Returns the validated chunk descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ValidatedXorbChunk {
        &self.descriptor
    }

    /// Returns decoded chunk bytes.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug, Error)]
pub enum XorbInvalidFormatError {
    #[error("serialized xorb failed structural validation")]
    StructuralValidationFailed,
    #[error("serialized xorb metadata section lengths disagreed")]
    MetadataSectionLengthMismatch,
    #[error("serialized xorb contained non-monotonic chunk boundaries")]
    NonMonotonicChunkBoundaries,
    #[error("serialized xorb packed content length disagreed with footer metadata")]
    PackedContentLengthMismatch,
    #[error("serialized xorb packed chunk length overflowed")]
    PackedChunkLengthOverflow,
    #[error("serialized xorb chunk payload did not match footer metadata")]
    ChunkPayloadMetadataMismatch,
    #[error("serialized xorb chunk payload hash did not match footer metadata")]
    ChunkPayloadHashMismatch,
    #[error("serialized xorb decoded chunk length disagreed with footer metadata")]
    DecodedChunkLengthMismatch,
    #[error("xorb core parser rejected malformed data")]
    CoreMalformedData,
    #[error("xorb core parser rejected serialized data")]
    CoreRejectedData,
    #[error("xorb hash bytes could not be converted into a merkle hash")]
    XorbHashConversionFailed,
    #[error("xorb chunk hash bytes could not be converted into protocol hash bytes")]
    ChunkHashConversionFailed,
}

/// Failure while parsing or validating a serialized xorb.
#[derive(Debug, Error)]
pub enum XorbParseError {
    /// The xorb structure was malformed or internally inconsistent.
    #[error("serialized xorb was invalid")]
    InvalidFormat(#[from] XorbInvalidFormatError),
    /// The serialized xorb hash did not match the expected hash.
    #[error("serialized xorb hash did not match the requested xorb hash")]
    HashMismatch,
    /// A numeric value exceeded Shardline's supported bounds.
    #[error("xorb parsing numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// The underlying reader failed.
    #[error("xorb parsing io failed")]
    Io(#[from] IoError),
}

/// Failure while visiting decoded xorb chunks.
#[derive(Debug, Error)]
pub enum XorbVisitError<VisitorError> {
    /// The serialized xorb could not be parsed or validated.
    #[error(transparent)]
    Parse(#[from] XorbParseError),
    /// The caller-provided chunk visitor failed.
    #[error("serialized xorb visitor failed")]
    Visitor(#[source] VisitorError),
}

/// Validates a serialized Xet xorb against the requested content hash and extracts
/// trusted chunk metadata.
///
/// # Errors
///
/// Returns [`XorbParseError`] when the xorb cannot be parsed, its structure is
/// malformed, numeric conversions overflow supported bounds, I/O fails, or the
/// serialized xorb hash differs from `expected_hash`.
pub fn validate_serialized_xorb<R: Read + Seek>(
    reader: &mut R,
    expected_hash: ShardlineHash,
) -> Result<ValidatedXorb, XorbParseError> {
    reader
        .seek(SeekFrom::Start(0))
        .map_err(XorbParseError::from)?;
    let expected_merkle_hash = shardline_hash_to_merkle_hash(expected_hash)?;
    let parsed = XorbObject::deserialize(reader).map_err(|error| map_core_error(&error))?;
    if parsed.info.xorb_hash != expected_merkle_hash {
        return Err(XorbParseError::HashMismatch);
    }
    reader
        .seek(SeekFrom::Start(0))
        .map_err(XorbParseError::from)?;
    let validated = XorbObject::validate_xorb_object(reader, &expected_merkle_hash)
        .map_err(|error| map_core_error(&error))?;
    let Some(validated) = validated else {
        return Err(XorbInvalidFormatError::StructuralValidationFailed.into());
    };
    let total_length = reader.seek(SeekFrom::End(0))?;
    let packed_content_length = u64::from(
        validated
            .get_contents_length()
            .map_err(|error| map_core_error(&error))?,
    );
    let num_chunks = usize::try_from(validated.info.num_chunks)?;
    if validated.info.chunk_hashes.len() != num_chunks
        || validated.info.chunk_boundary_offsets.len() != num_chunks
        || validated.info.unpacked_chunk_offsets.len() != num_chunks
    {
        return Err(XorbInvalidFormatError::MetadataSectionLengthMismatch.into());
    }

    let mut chunks = Vec::with_capacity(num_chunks);
    let mut packed_start = 0_u64;
    let mut unpacked_start = 0_u64;

    for index in 0..num_chunks {
        let (packed_end, unpacked_end, hash) = validated_chunk_footer_at(&validated, index)?;
        if packed_end <= packed_start || unpacked_end <= unpacked_start {
            return Err(XorbInvalidFormatError::NonMonotonicChunkBoundaries.into());
        }
        chunks.push(ValidatedXorbChunk::new(
            hash,
            packed_start,
            packed_end,
            unpacked_start,
            unpacked_end,
        ));
        packed_start = packed_end;
        unpacked_start = unpacked_end;
    }

    if packed_start != packed_content_length {
        return Err(XorbInvalidFormatError::PackedContentLengthMismatch.into());
    }

    Ok(ValidatedXorb::new(
        expected_hash,
        total_length,
        packed_content_length,
        unpacked_start,
        chunks,
    ))
}

/// Decodes the packed chunk stream of a previously validated serialized xorb.
///
/// # Errors
///
/// Returns [`XorbParseError`] when the reader cannot be rewound, chunk payloads
/// fail to decode, numeric conversions overflow supported bounds, or decoded chunk
/// lengths disagree with the validated xorb footer metadata.
pub fn decode_serialized_xorb_chunks<R: Read + Seek>(
    reader: &mut R,
    validated: &ValidatedXorb,
) -> Result<Vec<DecodedXorbChunk>, XorbParseError> {
    let mut decoded = Vec::with_capacity(validated.chunks().len());
    try_for_each_serialized_xorb_chunk(reader, validated, |chunk| {
        decoded.push(chunk);
        Ok::<(), XorbParseError>(())
    })
    .map_err(|error| match error {
        XorbVisitError::Parse(error) | XorbVisitError::Visitor(error) => error,
    })?;

    Ok(decoded)
}

/// Decodes the packed chunk stream of a previously validated serialized xorb and
/// passes each decoded chunk to `visitor` before decoding the next chunk.
///
/// # Errors
///
/// Returns [`XorbVisitError`] when xorb parsing fails or when the visitor rejects a
/// decoded chunk.
pub fn try_for_each_serialized_xorb_chunk<R, F, VisitorError>(
    reader: &mut R,
    validated: &ValidatedXorb,
    mut visitor: F,
) -> Result<(), XorbVisitError<VisitorError>>
where
    R: Read + Seek,
    F: FnMut(DecodedXorbChunk) -> Result<(), VisitorError>,
{
    reader
        .seek(SeekFrom::Start(0))
        .map_err(XorbParseError::from)?;
    let mut packed_end = 0_u64;

    for descriptor in validated.chunks() {
        let (data, packed_len, unpacked_len) =
            deserialize_chunk(reader).map_err(|error| map_core_error(&error))?;
        let packed_len = u64::try_from(packed_len).map_err(XorbParseError::from)?;
        let unpacked_len = u64::from(unpacked_len);
        let next_packed_end = packed_end.checked_add(packed_len).ok_or_else(|| {
            XorbParseError::from(XorbInvalidFormatError::PackedChunkLengthOverflow)
        })?;
        if descriptor.packed_start() != packed_end
            || descriptor.packed_end() != next_packed_end
            || descriptor.unpacked_len() != unpacked_len
        {
            return Err(
                XorbParseError::from(XorbInvalidFormatError::ChunkPayloadMetadataMismatch).into(),
            );
        }
        let actual_hash = merkle_hash_to_shardline_hash(compute_data_hash(&data))?;
        if descriptor.hash() != actual_hash {
            return Err(
                XorbParseError::from(XorbInvalidFormatError::ChunkPayloadHashMismatch).into(),
            );
        }
        visitor(DecodedXorbChunk::new(descriptor.clone(), data))
            .map_err(XorbVisitError::Visitor)?;
        packed_end = next_packed_end;
    }

    if packed_end != validated.packed_content_length() {
        return Err(
            XorbParseError::from(XorbInvalidFormatError::DecodedChunkLengthMismatch).into(),
        );
    }

    Ok(())
}

fn validated_chunk_footer_at(
    validated: &XorbObject,
    index: usize,
) -> Result<(u64, u64, ShardlineHash), XorbParseError> {
    let packed_end = u64::from(
        *validated
            .info
            .chunk_boundary_offsets
            .get(index)
            .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?,
    );
    let unpacked_end = u64::from(
        *validated
            .info
            .unpacked_chunk_offsets
            .get(index)
            .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?,
    );
    let hash = merkle_hash_to_shardline_hash(*validated.info.chunk_hashes.get(index).ok_or_else(
        || XorbParseError::from(XorbInvalidFormatError::MetadataSectionLengthMismatch),
    )?)?;

    Ok((packed_end, unpacked_end, hash))
}

fn map_core_error(error: &CoreError) -> XorbParseError {
    if matches!(&error, CoreError::HashMismatch) {
        return XorbParseError::HashMismatch;
    }

    if let CoreError::MalformedData(_message) = &error {
        return XorbInvalidFormatError::CoreMalformedData.into();
    }

    if let Some(io_error) =
        StdError::source(&error).and_then(|source| source.downcast_ref::<IoError>())
    {
        return XorbParseError::Io(IoError::new(io_error.kind(), "xorb core parser io failed"));
    }

    XorbInvalidFormatError::CoreRejectedData.into()
}

fn shardline_hash_to_merkle_hash(hash: ShardlineHash) -> Result<MerkleHash, XorbParseError> {
    MerkleHash::from_slice(hash.as_bytes())
        .map_err(|_error| XorbInvalidFormatError::XorbHashConversionFailed.into())
}

fn merkle_hash_to_shardline_hash(hash: MerkleHash) -> Result<ShardlineHash, XorbParseError> {
    let bytes: [u8; 32] = hash
        .as_bytes()
        .try_into()
        .map_err(|_error| XorbInvalidFormatError::ChunkHashConversionFailed)?;
    Ok(ShardlineHash::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use xet_core_structures::{
        merklehash::{compute_data_hash, xorb_hash},
        xorb_object::{
            CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
        },
    };

    use super::{
        ValidatedXorb, ValidatedXorbChunk, XorbInvalidFormatError, XorbParseError, XorbVisitError,
        decode_serialized_xorb_chunks, merkle_hash_to_shardline_hash,
        try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
    };
    use crate::ShardlineHash;

    #[test]
    fn validate_serialized_xorb_reports_chunk_metadata_and_decodes_bytes() {
        let first = b"hello ".to_vec();
        let second = b"world".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first.len()).unwrap_or(0)),
            (second_hash, u64::try_from(second.len()).unwrap_or(0)),
        ]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            [first.clone(), second.clone()].concat(),
            vec![
                (first_hash, u32::try_from(first.len()).unwrap_or(0)),
                (
                    second_hash,
                    u32::try_from(first.len() + second.len()).unwrap_or(0),
                ),
            ],
            CompressionScheme::LZ4,
        );

        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };

        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash);

        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        assert_eq!(validated.hash(), expected_hash);
        assert_eq!(validated.chunks().len(), 2);
        assert_eq!(validated.unpacked_length(), 11);

        let decoded = decode_serialized_xorb_chunks(&mut reader, &validated);
        assert!(decoded.is_ok());
        let Ok(decoded) = decoded else {
            return;
        };
        assert_eq!(
            decoded.first().map(|chunk| chunk.data()),
            Some(first.as_slice())
        );
        assert_eq!(
            decoded.get(1).map(|chunk| chunk.data()),
            Some(second.as_slice())
        );
    }

    #[test]
    fn validate_serialized_xorb_rejects_wrong_hash() {
        let data = b"hello".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u32::try_from(data.len()).unwrap_or(0))],
            CompressionScheme::None,
        );

        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let wrong_hash = ShardlineHash::from_bytes([9; 32]);
        let mut reader = Cursor::new(serialized.serialized_data);
        let result = validate_serialized_xorb(&mut reader, wrong_hash);

        assert!(matches!(result, Err(super::XorbParseError::HashMismatch)));
    }

    #[test]
    fn validate_serialized_xorb_rejects_truncated_payload() {
        let data = b"hello".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u32::try_from(data.len()).unwrap_or(0))],
            CompressionScheme::None,
        );

        assert!(serialized.is_ok());
        let Ok(mut serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };
        let removed = serialized.serialized_data.pop();
        assert!(removed.is_some());

        let mut reader = Cursor::new(serialized.serialized_data);
        let result = validate_serialized_xorb(&mut reader, expected_hash);

        assert!(result.is_err());
    }

    #[test]
    fn decode_serialized_xorb_rejects_descriptor_hash_mismatch() {
        let data = b"hello".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data,
            vec![(chunk_hash, 5)],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };
        let mut reader = Cursor::new(serialized.serialized_data.clone());
        let validated = validate_serialized_xorb(&mut reader, expected_hash);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let first_chunk = validated.chunks().first();
        assert!(first_chunk.is_some());
        let Some(first_chunk) = first_chunk else {
            return;
        };
        let forged_chunk = ValidatedXorbChunk::new(
            ShardlineHash::from_bytes([9; 32]),
            first_chunk.packed_start(),
            first_chunk.packed_end(),
            first_chunk.unpacked_start(),
            first_chunk.unpacked_end(),
        );
        let forged_validation = ValidatedXorb::new(
            validated.hash(),
            validated.total_length(),
            validated.packed_content_length(),
            validated.unpacked_length(),
            vec![forged_chunk],
        );
        let result = decode_serialized_xorb_chunks(
            &mut Cursor::new(serialized.serialized_data),
            &forged_validation,
        );

        assert!(matches!(
            result,
            Err(XorbParseError::InvalidFormat(
                XorbInvalidFormatError::ChunkPayloadHashMismatch
            ))
        ));
    }

    #[test]
    fn serialized_xorb_chunk_visitor_error_propagates_without_continuing() {
        let first = b"hello ".to_vec();
        let second = b"world".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first.len()).unwrap_or(0)),
            (second_hash, u64::try_from(second.len()).unwrap_or(0)),
        ]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            [first, second].concat(),
            vec![(first_hash, 6), (second_hash, 11)],
            CompressionScheme::LZ4,
        );

        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };
        let mut visited = 0_u8;

        let result = try_for_each_serialized_xorb_chunk(&mut reader, &validated, |_chunk| {
            visited = visited.saturating_add(1);
            Err::<(), &str>("stop")
        });

        assert!(matches!(result, Err(XorbVisitError::Visitor("stop"))));
        assert_eq!(visited, 1);
    }
}
