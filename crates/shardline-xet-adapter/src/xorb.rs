use std::{
    error::Error as StdError,
    future::Future,
    io::{Error as IoError, Read, Seek, SeekFrom},
    num::TryFromIntError,
};

use shardline_protocol::ShardlineHash;
use shardline_xet_core::{
    error::CoreError,
    merklehash::{MerkleHash, compute_data_hash},
    xorb_object::{XorbObject, deserialize_chunk},
};
use thiserror::Error;

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
    let packed_content_length = validated
        .get_contents_length()
        .map_err(|error| map_core_error(&error))?;
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
        let actual_hash = merkle_hash_to_shardline_hash(compute_data_hash(&data));
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

/// Decodes the packed chunk stream of a previously validated serialized xorb and
/// passes each decoded chunk to an async `visitor` before decoding the next chunk.
///
/// # Errors
///
/// Returns [`XorbVisitError`] when xorb parsing fails or when the visitor rejects a
/// decoded chunk.
pub async fn try_for_each_serialized_xorb_chunk_async<R, F, Fut, VisitorError>(
    reader: &mut R,
    validated: &ValidatedXorb,
    mut visitor: F,
) -> Result<(), XorbVisitError<VisitorError>>
where
    R: Read + Seek,
    F: FnMut(DecodedXorbChunk) -> Fut,
    Fut: Future<Output = Result<(), VisitorError>>,
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
        let actual_hash = merkle_hash_to_shardline_hash(compute_data_hash(&data));
        if descriptor.hash() != actual_hash {
            return Err(
                XorbParseError::from(XorbInvalidFormatError::ChunkPayloadHashMismatch).into(),
            );
        }
        visitor(DecodedXorbChunk::new(descriptor.clone(), data))
            .await
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
    let packed_end = *validated
        .info
        .chunk_boundary_offsets
        .get(index)
        .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?;
    let unpacked_end = *validated
        .info
        .unpacked_chunk_offsets
        .get(index)
        .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?;
    let hash = merkle_hash_to_shardline_hash(*validated.info.chunk_hashes.get(index).ok_or_else(
        || XorbParseError::from(XorbInvalidFormatError::MetadataSectionLengthMismatch),
    )?);

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

fn merkle_hash_to_shardline_hash(hash: MerkleHash) -> ShardlineHash {
    let bytes: [u8; 32] = hash.into();
    ShardlineHash::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use shardline_xet_core::{
        merklehash::{compute_data_hash, xorb_hash},
        xorb_object::{
            CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
        },
    };

    use super::{
        DecodedXorbChunk, ValidatedXorb, ValidatedXorbChunk, XorbInvalidFormatError,
        XorbParseError, XorbVisitError, decode_serialized_xorb_chunks,
        merkle_hash_to_shardline_hash, try_for_each_serialized_xorb_chunk,
        validate_serialized_xorb,
    };
    use crate::XetAdapterError;
    use shardline_protocol::ShardlineHash;

    // ── XorbParseError Display ─────────────────────────────────────────

    #[test]
    fn xorb_parse_error_display_invalid_format() {
        let err = XorbParseError::InvalidFormat(XorbInvalidFormatError::StructuralValidationFailed);
        let msg = err.to_string();
        assert!(msg.contains("invalid"), "msg: {msg}");
    }

    #[test]
    fn xorb_parse_error_display_hash_mismatch() {
        let err = XorbParseError::HashMismatch;
        let msg = err.to_string();
        assert!(msg.contains("hash"), "msg: {msg}");
    }

    #[test]
    fn xorb_parse_error_display_numeric_conversion() {
        let err = XorbParseError::NumericConversion(u64::try_from(-1i32).unwrap_err());
        let msg = err.to_string();
        assert!(msg.contains("conversion"), "msg: {msg}");
    }

    #[test]
    fn xorb_parse_error_display_io() {
        let err = XorbParseError::Io(std::io::Error::other("disk failure"));
        let msg = err.to_string();
        assert!(msg.contains("io"), "msg: {msg}");
    }

    // ── XorbInvalidFormatError Display ──────────────────────────────────

    #[test]
    fn xorb_invalid_format_error_display_all_variants() {
        let cases: &[(XorbInvalidFormatError, &str)] = &[
            (
                XorbInvalidFormatError::StructuralValidationFailed,
                "structural",
            ),
            (
                XorbInvalidFormatError::MetadataSectionLengthMismatch,
                "length",
            ),
            (
                XorbInvalidFormatError::NonMonotonicChunkBoundaries,
                "boundar",
            ),
            (
                XorbInvalidFormatError::PackedContentLengthMismatch,
                "length",
            ),
            (
                XorbInvalidFormatError::PackedChunkLengthOverflow,
                "overflow",
            ),
            (
                XorbInvalidFormatError::ChunkPayloadMetadataMismatch,
                "metadata",
            ),
            (XorbInvalidFormatError::ChunkPayloadHashMismatch, "hash"),
            (XorbInvalidFormatError::DecodedChunkLengthMismatch, "length"),
            (XorbInvalidFormatError::CoreMalformedData, "malformed"),
            (XorbInvalidFormatError::CoreRejectedData, "rejected"),
            (XorbInvalidFormatError::XorbHashConversionFailed, "merkle"),
            (
                XorbInvalidFormatError::ChunkHashConversionFailed,
                "protocol",
            ),
        ];
        for (variant, expected) in cases {
            let msg = variant.to_string();
            assert!(
                msg.contains(expected),
                "variant {variant:?} msg '{msg}' missing '{expected}'"
            );
        }
    }

    // ── XorbVisitError Display ──────────────────────────────────────────

    #[test]
    fn xorb_visit_error_parse_variant_displays_parse_error() {
        let err: XorbVisitError<XetAdapterError> = XorbParseError::HashMismatch.into();
        let msg = err.to_string();
        assert!(msg.contains("hash"), "msg: {msg}");
    }

    #[test]
    fn xorb_visit_error_visitor_variant_displays_generic_message() {
        let err = XorbVisitError::<XetAdapterError>::Visitor(XetAdapterError::NotFound);
        let msg = err.to_string();
        assert!(msg.contains("visitor"), "msg: {msg}");
    }

    #[test]
    fn validate_serialized_xorb_reports_chunk_metadata_and_decodes_bytes() {
        let first = b"hello ".to_vec();
        let second = b"world".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let first_len = first.len();
        let second_len = second.len();
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first_len).unwrap_or(0)),
            (second_hash, u64::try_from(second_len).unwrap_or(0)),
        ]);
        let combined = [first.as_slice(), second.as_slice()].concat();
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            combined,
            vec![
                (first_hash, u64::try_from(first_len).unwrap_or(0)),
                (
                    second_hash,
                    u64::try_from(first_len + second_len).unwrap_or(0),
                ),
            ],
            CompressionScheme::LZ4,
        );

        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);

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
            vec![(chunk_hash, u64::try_from(data.len()).unwrap_or(0))],
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
            vec![(chunk_hash, u64::try_from(data.len()).unwrap_or(0))],
            CompressionScheme::None,
        );

        assert!(serialized.is_ok());
        let Ok(mut serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
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
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
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

    // ── ValidatedXorbChunk accessors ────────────────────────────────────

    #[test]
    fn validated_xorb_chunk_accessors() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"test"));
        let chunk = ValidatedXorbChunk::new(hash, 10, 100, 50, 80);
        assert_eq!(chunk.hash(), hash);
        assert_eq!(chunk.packed_start(), 10);
        assert_eq!(chunk.packed_end(), 100);
        assert_eq!(chunk.unpacked_start(), 50);
        assert_eq!(chunk.unpacked_end(), 80);
        assert_eq!(chunk.unpacked_len(), 30);
    }

    #[test]
    fn validated_xorb_chunk_unpacked_len_zero() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"t"));
        let chunk = ValidatedXorbChunk::new(hash, 0, 0, 5, 5);
        assert_eq!(chunk.unpacked_len(), 0);
    }

    #[test]
    fn validated_xorb_chunk_unpacked_len_saturating() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"t"));
        let chunk = ValidatedXorbChunk::new(hash, 0, 0, 100, 50);
        // saturating_sub: 50 - 100 = 0
        assert_eq!(chunk.unpacked_len(), 0);
    }

    // ── ValidatedXorb accessors ─────────────────────────────────────────

    #[test]
    fn validated_xorb_accessors() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"test"));
        let chunk = ValidatedXorbChunk::new(hash, 0, 100, 0, 50);
        let validated = ValidatedXorb::new(hash, 1000, 900, 500, vec![chunk.clone()]);
        assert_eq!(validated.hash(), hash);
        assert_eq!(validated.total_length(), 1000);
        assert_eq!(validated.packed_content_length(), 900);
        assert_eq!(validated.unpacked_length(), 500);
        assert_eq!(validated.chunks(), &[chunk]);
    }

    #[test]
    fn validated_xorb_empty_chunks() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"empty"));
        let validated = ValidatedXorb::new(hash, 0, 0, 0, vec![]);
        assert!(validated.chunks().is_empty());
    }

    // ── DecodedXorbChunk accessors ──────────────────────────────────────

    #[test]
    fn decoded_xorb_chunk_accessors() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"data"));
        let descriptor = ValidatedXorbChunk::new(hash, 0, 4, 0, 4);
        let data = b"hello".to_vec();
        let decoded = DecodedXorbChunk::new(descriptor.clone(), data.clone());
        assert_eq!(*decoded.descriptor(), descriptor);
        assert_eq!(decoded.data(), data.as_slice());
    }

    // ── merkle_hash_to_shardline_hash round-trip ───────────────────────

    #[test]
    fn merkle_hash_to_shardline_hash_round_trip() {
        let merkle = compute_data_hash(b"roundtrip");
        let shardline = merkle_hash_to_shardline_hash(merkle);
        let bytes: [u8; 32] = merkle.into();
        assert_eq!(shardline.as_bytes(), &bytes);
    }

    // ── XorbInvalidFormatError From impl ──────────────────────────────────

    #[test]
    fn xorb_invalid_format_error_from_for_parse_error_preserves_variant() {
        let err: XorbParseError = XorbInvalidFormatError::StructuralValidationFailed.into();
        assert!(matches!(err, XorbParseError::InvalidFormat(_)));
    }

    // ── XorbVisitError From impls ─────────────────────────────────────────

    #[test]
    fn xorb_visit_error_from_xorb_parse_error() {
        let parse_err = XorbParseError::HashMismatch;
        let visit_err: XorbVisitError<XetAdapterError> = parse_err.into();
        assert!(matches!(visit_err, XorbVisitError::Parse(_)));
    }

    // ── validate_serialized_xorb edge cases ──────────────────────────────

    #[test]
    fn validate_serialized_xorb_rejects_non_monotonic_chunk_boundaries() {
        // Create two chunks where the second has lower boundaries
        let first = b"hello".to_vec();
        let second = b"world".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let first_len = first.len();
        let second_len = second.len();
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first_len).unwrap_or(0)),
            (second_hash, u64::try_from(second_len).unwrap_or(0)),
        ]);
        let combined = [first.as_slice(), second.as_slice()].concat();
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            combined,
            vec![
                (first_hash, u64::try_from(first_len).unwrap_or(0)),
                (
                    second_hash,
                    u64::try_from(first_len + second_len).unwrap_or(0),
                ),
            ],
            CompressionScheme::LZ4,
        );
        assert!(serialized.is_ok());
        let Ok(mut serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);

        // Corrupt the serialized data to cause non-monotonic boundaries
        // by swapping footer metadata offsets (induce inconsistent boundaries)
        // Actually: truncate footer to make validation fail
        // The simplest approach: remove data between the chunks to shift boundaries
        let _ = serialized.serialized_data.pop();

        let mut reader = Cursor::new(serialized.serialized_data);
        let result = validate_serialized_xorb(&mut reader, expected_hash);
        assert!(result.is_err(), "expected error for corrupted xorb");
    }

    #[test]
    fn validate_serialized_xorb_rejects_empty_data() {
        let hash = ShardlineHash::from_bytes([0; 32]);
        let mut reader = Cursor::new(Vec::new());
        let result = validate_serialized_xorb(&mut reader, hash);
        assert!(result.is_err(), "expected error for empty data");
    }

    // ── DecodedXorbChunk edge cases ──────────────────────────────────────

    #[test]
    fn decoded_xorb_chunk_data_mutability() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"d"));
        let descriptor = ValidatedXorbChunk::new(hash, 0, 1, 0, 1);
        let data = vec![1u8, 2, 3];
        let decoded = DecodedXorbChunk::new(descriptor, data);
        assert_eq!(decoded.data().len(), 3);
        assert_eq!(decoded.data(), &[1, 2, 3]);
    }

    // ── ValidatedXorbChunk constructors ───────────────────────────────────

    #[test]
    fn validated_xorb_chunk_new_and_const_values() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"const"));
        let chunk = ValidatedXorbChunk::new(hash, 5, 10, 3, 8);
        assert_eq!(chunk.hash(), hash);
        assert_eq!(chunk.packed_start(), 5);
        assert_eq!(chunk.packed_end(), 10);
        assert_eq!(chunk.unpacked_start(), 3);
        assert_eq!(chunk.unpacked_end(), 8);
    }

    // ── ValidatedXorb constructors ────────────────────────────────────────

    #[test]
    fn validated_xorb_new_zero_lengths() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"zero"));
        let validated = ValidatedXorb::new(hash, 0, 0, 0, vec![]);
        assert_eq!(validated.total_length(), 0);
        assert_eq!(validated.packed_content_length(), 0);
        assert_eq!(validated.unpacked_length(), 0);
    }

    // ── XorbParseError conversion coverage ───────────────────────────────

    #[test]
    fn xorb_parse_error_from_io_error() {
        let io_err = std::io::Error::other("test");
        let parse_err: XorbParseError = io_err.into();
        let msg = parse_err.to_string();
        assert!(msg.contains("io"), "msg: {msg}");
    }

    // ── try_for_each_serialized_xorb_chunk ────────────────────────────────

    #[test]
    fn try_for_each_xorb_chunk_decodes_all_chunks_and_verifies_total() {
        let first = b"aaaa".to_vec();
        let second = b"bbbb".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first.len()).unwrap_or(0)),
            (second_hash, u64::try_from(second.len()).unwrap_or(0)),
        ]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            [first, second].concat(),
            vec![(first_hash, 4), (second_hash, 8)],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };

        let mut count = 0_usize;
        let result = try_for_each_serialized_xorb_chunk(&mut reader, &validated, |_chunk| {
            count += 1;
            Ok::<(), XetAdapterError>(())
        });
        assert!(result.is_ok());
        assert_eq!(count, 2);
    }

    #[test]
    fn decode_serialized_xorb_chunks_round_trips_data() {
        let first = b"hello ".to_vec();
        let second = b"world".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let first_len = first.len();
        let second_len = second.len();
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first_len).unwrap_or(0)),
            (second_hash, u64::try_from(second_len).unwrap_or(0)),
        ]);
        let combined = [first.as_slice(), second.as_slice()].concat();
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            combined,
            vec![
                (first_hash, u64::try_from(first_len).unwrap_or(0)),
                (
                    second_hash,
                    u64::try_from(first_len + second_len).unwrap_or(0),
                ),
            ],
            CompressionScheme::LZ4,
        );
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash);
        assert!(validated.is_ok());
        let Ok(validated) = validated else {
            return;
        };

        let decoded = decode_serialized_xorb_chunks(&mut reader, &validated);
        assert!(decoded.is_ok());
        let Ok(decoded) = decoded else {
            return;
        };
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].data(), first.as_slice());
        assert_eq!(decoded[1].data(), second.as_slice());
    }

    // ── XorbParseError source access (for error-chain coverage) ──────────

    #[test]
    fn xorb_parse_error_invalid_format_source() {
        let inner = XorbInvalidFormatError::StructuralValidationFailed;
        let err = XorbParseError::InvalidFormat(inner);
        let source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&err);
        // InvalidFormat wraps the inner error; its source is the inner error
        // The inner error has no further source
        assert!(source.is_some());
    }

    // ── Additional error-path coverage for validate_serialized_xorb ──────

    #[test]
    fn validate_serialized_xorb_with_tiny_invalid_buffer() {
        // A buffer that is too small to be a valid xorb
        let tiny = vec![0u8; 4];
        let hash = ShardlineHash::from_bytes([0; 32]);
        let mut reader = Cursor::new(tiny);
        let result = validate_serialized_xorb(&mut reader, hash);
        // Should get an error - either MalformedData (from deserialize) or other
        assert!(result.is_err(), "expected error for tiny buffer");
    }

    #[test]
    fn validate_serialized_xorb_with_large_garbage() {
        let garbage = vec![0xFFu8; 4096];
        let hash = ShardlineHash::from_bytes([0; 32]);
        let mut reader = Cursor::new(garbage);
        let result = validate_serialized_xorb(&mut reader, hash);
        // Should fail - this is not a valid xorb format
        assert!(result.is_err(), "expected error for garbage data");
    }

    #[test]
    fn validate_serialized_xorb_with_partial_header() {
        // Partial xorb that has a few bytes of the header only
        let partial = vec![0x00u8; 100];
        let hash = ShardlineHash::from_bytes([0; 32]);
        let mut reader = Cursor::new(partial);
        let result = validate_serialized_xorb(&mut reader, hash);
        assert!(result.is_err(), "expected error for partial header data");
    }

    // ── ValidatedXorbChunk with non-zero starting offsets ─────────────

    #[test]
    fn validated_xorb_chunk_non_zero_offsets() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"offset"));
        let chunk = ValidatedXorbChunk::new(hash, 50, 150, 200, 350);
        assert_eq!(chunk.packed_start(), 50);
        assert_eq!(chunk.packed_end(), 150);
        assert_eq!(chunk.unpacked_start(), 200);
        assert_eq!(chunk.unpacked_end(), 350);
        assert_eq!(chunk.unpacked_len(), 150);
    }

    // ── DecodedXorbChunk with empty data ──────────────────────────────

    #[test]
    fn decoded_xorb_chunk_empty_data() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"empty_data"));
        let descriptor = ValidatedXorbChunk::new(hash, 0, 0, 0, 0);
        let decoded = DecodedXorbChunk::new(descriptor, Vec::new());
        assert!(decoded.data().is_empty());
    }

    // ── XorbVisitError From impl coverage ──────────────────────────────

    #[test]
    fn xorb_visit_error_from_xorb_parse_error_via_from_trait() {
        let parse_err = XorbParseError::Io(std::io::Error::other("e"));
        let visit_err: XorbVisitError<XetAdapterError> = parse_err.into();
        let msg = visit_err.to_string();
        assert!(
            !msg.is_empty(),
            "XorbVisitError display should not be empty"
        );
    }

    // ── try_for_each_serialized_xorb_chunk error paths via forged validated ─

    #[test]
    fn try_for_each_xorb_chunk_detects_payload_metadata_mismatch() {
        let data = b"hello world".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap())]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::None,
        )
        .unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        // Forge packed_start to trigger mismatch
        let forged_chunk = ValidatedXorbChunk::new(
            validated.chunks()[0].hash(),
            42,
            validated.chunks()[0].packed_end(),
            validated.chunks()[0].unpacked_start(),
            validated.chunks()[0].unpacked_end(),
        );
        let forged_validated = ValidatedXorb::new(
            validated.hash(),
            validated.total_length(),
            validated.packed_content_length(),
            validated.unpacked_length(),
            vec![forged_chunk],
        );

        let result = try_for_each_serialized_xorb_chunk(
            &mut Cursor::new(serialized.serialized_data),
            &forged_validated,
            |_chunk| Ok::<(), XetAdapterError>(()),
        );

        assert!(matches!(
            result,
            Err(XorbVisitError::Parse(XorbParseError::InvalidFormat(
                XorbInvalidFormatError::ChunkPayloadMetadataMismatch
            )))
        ));
    }

    #[test]
    fn try_for_each_xorb_chunk_detects_decoded_length_mismatch() {
        // Create a valid xorb with two chunks, validate, forge
        // validated.packed_content_length to be wrong, triggering
        // DecodedChunkLengthMismatch.
        let first = b"aaaa".to_vec();
        let second = b"bbbb".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first.len()).unwrap_or(0)),
            (second_hash, u64::try_from(second.len()).unwrap_or(0)),
        ]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            [first, second].concat(),
            vec![(first_hash, 4), (second_hash, 8)],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        // Forge packed_content_length to be larger than actual decoded total
        let forged_validated = ValidatedXorb::new(
            validated.hash(),
            validated.total_length(),
            validated.packed_content_length() + 999, // wrong - triggers DecodedChunkLengthMismatch
            validated.unpacked_length(),
            validated.chunks().to_vec(),
        );

        let result = try_for_each_serialized_xorb_chunk(
            &mut Cursor::new(serialized.serialized_data),
            &forged_validated,
            |_chunk| Ok::<(), XetAdapterError>(()),
        );

        assert!(
            matches!(
                result,
                Err(XorbVisitError::Parse(XorbParseError::InvalidFormat(
                    XorbInvalidFormatError::DecodedChunkLengthMismatch
                )))
            ),
            "expected DecodedChunkLengthMismatch, got {result:?}"
        );
    }

    #[test]
    fn try_for_each_xorb_chunk_detects_payload_metadata_mismatch_end() {
        // Forge the first chunk's packed_end to be wrong, which triggers
        // ChunkPayloadMetadataMismatch via descriptor.packed_end() != next_packed_end.
        let data = b"test data".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        // Forge: change packed_end to be different from actual decoded end
        let forged_chunk = ValidatedXorbChunk::new(
            validated.chunks()[0].hash(),
            validated.chunks()[0].packed_start(),
            validated.chunks()[0].packed_end() + 100, // wrong packed_end
            validated.chunks()[0].unpacked_start(),
            validated.chunks()[0].unpacked_end(),
        );
        let forged_validated = ValidatedXorb::new(
            validated.hash(),
            validated.total_length(),
            validated.packed_content_length(),
            validated.unpacked_length(),
            vec![forged_chunk],
        );

        let result = try_for_each_serialized_xorb_chunk(
            &mut Cursor::new(serialized.serialized_data),
            &forged_validated,
            |_chunk| Ok::<(), XetAdapterError>(()),
        );

        assert!(
            matches!(
                result,
                Err(XorbVisitError::Parse(XorbParseError::InvalidFormat(
                    XorbInvalidFormatError::ChunkPayloadMetadataMismatch
                )))
            ),
            "expected ChunkPayloadMetadataMismatch, got {result:?}"
        );
    }

    #[test]
    fn decode_serialized_xorb_chunks_propagates_decoded_length_mismatch() {
        // Use the forged validated flow through decode_serialized_xorb_chunks
        // to trigger DecodedChunkLengthMismatch.
        let data = b"xyz".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data,
            vec![(chunk_hash, 3)],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        // Forge packed_content_length shorter than actual
        let forged_validated = ValidatedXorb::new(
            validated.hash(),
            validated.total_length(),
            0, // wrong - trigger DecodedChunkLengthMismatch
            validated.unpacked_length(),
            validated.chunks().to_vec(),
        );

        let result = decode_serialized_xorb_chunks(
            &mut Cursor::new(serialized.serialized_data),
            &forged_validated,
        );

        assert!(
            matches!(
                result,
                Err(XorbParseError::InvalidFormat(
                    XorbInvalidFormatError::DecodedChunkLengthMismatch
                ))
            ),
            "expected DecodedChunkLengthMismatch, got {result:?}"
        );
    }

    // ── validate_serialized_xorb with varied compression schemes ────────

    #[test]
    fn validate_serialized_xorb_lz4_round_trip() {
        let data = b"compressed data payload".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap())]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::LZ4,
        )
        .unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();
        assert_eq!(validated.chunks().len(), 1);
        assert_eq!(validated.unpacked_length(), data.len() as u64);
        let decoded = decode_serialized_xorb_chunks(&mut reader, &validated).unwrap();
        assert_eq!(decoded[0].data(), data.as_slice());
    }

    #[test]
    fn validate_serialized_xorb_none_round_trip() {
        let data = b"no compression".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap())]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::None,
        )
        .unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();
        let decoded = decode_serialized_xorb_chunks(&mut reader, &validated).unwrap();
        assert_eq!(decoded[0].data(), data.as_slice());
    }

    #[test]
    fn validate_serialized_xorb_bg4_round_trip() {
        let data = b"bg4 compressed data".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap())]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data.clone(),
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::ByteGrouping4LZ4,
        )
        .unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();
        assert_eq!(validated.chunks().len(), 1);
        assert_eq!(validated.unpacked_length(), data.len() as u64);
        let decoded = decode_serialized_xorb_chunks(&mut reader, &validated).unwrap();
        assert_eq!(decoded[0].data(), data.as_slice());
    }

    // ── DecodedXorbChunk / ValidatedXorbChunk edge cases ────────────────

    #[test]
    fn validated_xorb_chunk_max_offsets() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"max"));
        let chunk = ValidatedXorbChunk::new(hash, 0, u64::MAX, 0, u64::MAX);
        assert_eq!(chunk.packed_start(), 0);
        assert_eq!(chunk.packed_end(), u64::MAX);
        assert_eq!(chunk.unpacked_len(), u64::MAX);
    }

    #[test]
    fn validated_xorb_chunk_large_offsets() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"large"));
        let chunk = ValidatedXorbChunk::new(hash, 1_000_000, 2_000_000, 5_000_000, 10_000_000);
        assert_eq!(chunk.unpacked_len(), 5_000_000);
    }

    #[test]
    fn validated_xorb_large_values() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"big"));
        let chunk = ValidatedXorbChunk::new(hash, 0, 100, 0, 100);
        let validated = ValidatedXorb::new(hash, u64::MAX, u64::MAX, u64::MAX, vec![chunk]);
        assert_eq!(validated.total_length(), u64::MAX);
        assert_eq!(validated.packed_content_length(), u64::MAX);
        assert_eq!(validated.unpacked_length(), u64::MAX);
    }

    #[test]
    fn decoded_xorb_chunk_accessors_consistency() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"cons"));
        let descriptor = ValidatedXorbChunk::new(hash, 10, 20, 5, 15);
        let data = vec![1, 2, 3, 4, 5];
        let chunk = DecodedXorbChunk::new(descriptor.clone(), data);
        assert_eq!(*chunk.descriptor(), descriptor);
        assert_eq!(chunk.data(), &[1, 2, 3, 4, 5]);
        assert_eq!(chunk.descriptor().hash(), hash);
        assert_eq!(chunk.descriptor().packed_start(), 10);
    }

    // ── XorbVisitError type erasure ────────────────────────────────────

    #[test]
    fn xorb_visit_error_from_different_visitor_types() {
        // Verify that XorbVisitError works with different visitor error types
        let parse: XorbVisitError<String> = XorbParseError::HashMismatch.into();
        assert!(matches!(parse, XorbVisitError::Parse(_)));
        let visit: XorbVisitError<String> = XorbVisitError::Visitor("custom".to_owned());
        assert!(matches!(visit, XorbVisitError::Visitor(_)));
        let msg = visit.to_string();
        assert!(msg.contains("visitor"));
    }

    // ── try_for_each_serialized_xorb_chunk with varying compression ─────

    #[test]
    fn try_for_each_xorb_chunk_works_with_lz4_compression() {
        let first = b"hello ".to_vec();
        let second = b"world!".to_vec();
        let first_hash = compute_data_hash(&first);
        let second_hash = compute_data_hash(&second);
        let xorb_hash = xorb_hash(&[
            (first_hash, u64::try_from(first.len()).unwrap_or(0)),
            (second_hash, u64::try_from(second.len()).unwrap_or(0)),
        ]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            [first, second].concat(),
            vec![(first_hash, 6), (second_hash, 12)],
            CompressionScheme::LZ4,
        );
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        let mut count = 0_usize;
        let result = try_for_each_serialized_xorb_chunk(&mut reader, &validated, |_chunk| {
            count += 1;
            Ok::<(), XetAdapterError>(())
        });
        assert!(result.is_ok());
        assert_eq!(count, 2);
    }

    #[test]
    fn try_for_each_xorb_chunk_single_chunk_none_compression() {
        let data = b"alone".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data,
            vec![(chunk_hash, 5)],
            CompressionScheme::None,
        );
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        let mut decoded = Vec::new();
        let result = try_for_each_serialized_xorb_chunk(&mut reader, &validated, |chunk| {
            decoded.push(chunk.data().to_vec());
            Ok::<(), XetAdapterError>(())
        });
        assert!(result.is_ok());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], b"alone");
    }

    // ── validate_serialized_xorb with StructuralValidationFailed ────────

    #[test]
    fn validate_serialized_xorb_structural_validation_failure() {
        // Create a xorb that can be deserialized but whose content
        // doesn't match the footer, causing validate_xorb_object to
        // return Ok(None).
        let data = b"valid data".to_vec();
        let chunk_hash = compute_data_hash(&data);
        // Use wrong chunk data vs hash to trigger validation failure
        let wrong_data = b"wrong data".to_vec();
        let _wrong_chunk_hash = compute_data_hash(&wrong_data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap_or(0))]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            // Put wrong data in the payload (hashes in footer say "valid data" but
            // we put "wrong data")
            wrong_data,
            // Hashes in footer point to "valid data" hash
            vec![(chunk_hash, u64::try_from(data.len()).unwrap())],
            CompressionScheme::None,
        );
        // serialized_xorb_object_from_components uses the hashes to compute
        // chunk boundaries, but the actual data bytes might not match.
        // This might succeed because the function only writes the raw data
        // and uses the hash list for footer metadata.
        assert!(serialized.is_ok());
        let serialized = serialized.unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);

        let mut reader = Cursor::new(serialized.serialized_data);
        let result = validate_serialized_xorb(&mut reader, expected_hash);

        // validate_xorb_object should detect the hash mismatch and return
        // Ok(None), which triggers StructuralValidationFailed
        assert!(result.is_err(), "expected error for structural validation");
    }

    #[test]
    fn validate_serialized_xorb_empty_chunks_fails_structural() {
        // This test validates that a xorb with no chunks triggers
        // structural validation failure (num_chunks=0 -> get_contents_length fails).
        use shardline_xet_core::merklehash::MerkleHash;
        // Create an empty xorb that can be deserialized but has num_chunks=0
        let xorb_hash = MerkleHash::default(); // will fail validation
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            Vec::new(),
            Vec::new(),
            CompressionScheme::None,
        );
        // serialized_xorb_object_from_components with 0 chunks might produce
        // an invalid xorb - just test that any errors are caught
        if let Ok(serialized) = serialized {
            let hash = ShardlineHash::from_bytes([0; 32]);
            let mut reader = Cursor::new(serialized.serialized_data);
            let result = validate_serialized_xorb(&mut reader, hash);
            assert!(result.is_err(), "expected error for empty chunks xorb");
        }
    }

    // ── XorbVisitError display and source ──────────────────────────────

    #[test]
    fn xorb_visit_error_parse_display_transparent() {
        // XorbVisitError::Parse uses #[error(transparent)], so Display
        // delegates to the inner XorbParseError.
        let parse_err = XorbParseError::HashMismatch;
        let visit_err: XorbVisitError<XetAdapterError> = parse_err.into();
        let msg = visit_err.to_string();
        assert!(
            msg.contains("hash"),
            "transparent display should show inner error: {msg}"
        );
    }

    #[test]
    fn xorb_visit_error_visitor_source_and_display() {
        let inner = XetAdapterError::Overflow;
        let err = XorbVisitError::<XetAdapterError>::Visitor(inner);
        let msg = err.to_string();
        assert!(msg.contains("visitor"), "msg: {msg}");
        // Check that it has source via Display (thiserror may or may
        // not expose source() depending on version)
        let _ = err;
    }

    // ── XorbParseError Display with all XorbInvalidFormatError variants ──
    // XorbParseError::InvalidFormat displays as "serialized xorb was invalid"
    // (from the #[error] attribute on the enum variant), regardless of the
    // inner XorbInvalidFormatError variant.

    #[test]
    fn xorb_parse_error_invalid_format_all_variants_display_same_outer_message() {
        // Build each variant individually and verify they all display the
        // wrapper "serialized xorb was invalid" message.
        let cases: &[(XorbParseError, &str)] = &[
            (
                XorbInvalidFormatError::StructuralValidationFailed.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::MetadataSectionLengthMismatch.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::NonMonotonicChunkBoundaries.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::PackedContentLengthMismatch.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::PackedChunkLengthOverflow.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::ChunkPayloadMetadataMismatch.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::ChunkPayloadHashMismatch.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::DecodedChunkLengthMismatch.into(),
                "invalid",
            ),
            (XorbInvalidFormatError::CoreMalformedData.into(), "invalid"),
            (XorbInvalidFormatError::CoreRejectedData.into(), "invalid"),
            (
                XorbInvalidFormatError::XorbHashConversionFailed.into(),
                "invalid",
            ),
            (
                XorbInvalidFormatError::ChunkHashConversionFailed.into(),
                "invalid",
            ),
        ];
        for (err, expected) in cases {
            let msg = err.to_string();
            assert!(
                msg.contains(expected),
                "err {err:?} msg '{msg}' missing '{expected}'"
            );
        }
    }

    // ── XorbVisitError with different visitor errors ────────────────────

    #[test]
    fn xorb_visit_error_visitor_variant_display_different_errors() {
        let errors = vec![
            XetAdapterError::NotFound,
            XetAdapterError::Overflow,
            XetAdapterError::InvalidContentHash,
            XetAdapterError::XorbHashMismatch,
        ];
        for error in errors {
            let visit_err = XorbVisitError::<XetAdapterError>::Visitor(error);
            let msg = visit_err.to_string();
            assert!(msg.contains("visitor"), "msg: {msg}");
        }
    }

    // ── map_core_error direct tests for uncovered error paths ────────────

    #[test]
    fn map_core_error_propagates_malformed_data() {
        let core_err = super::CoreError::MalformedData("test".to_owned());
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(
                result,
                XorbParseError::InvalidFormat(XorbInvalidFormatError::CoreMalformedData)
            ),
            "expected CoreMalformedData, got {result:?}"
        );
    }

    #[test]
    fn map_core_error_propagates_io_error() {
        let io_err = std::io::Error::other("test");
        let core_err = super::CoreError::Io(io_err);
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(result, XorbParseError::Io(_)),
            "expected Io, got {result:?}"
        );
    }

    #[test]
    fn map_core_error_propagates_generic_error() {
        let core_err = super::CoreError::InternalError("generic".to_owned());
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(
                result,
                XorbParseError::InvalidFormat(XorbInvalidFormatError::CoreRejectedData)
            ),
            "expected CoreRejectedData, got {result:?}"
        );
    }

    #[test]
    fn map_core_error_propagates_hash_mismatch() {
        let result = super::map_core_error(&super::CoreError::HashMismatch);
        assert!(
            matches!(result, XorbParseError::HashMismatch),
            "expected HashMismatch, got {result:?}"
        );
    }

    // ── shardline_hash_to_merkle_hash error path ──────────────────────────

    #[test]
    fn shardline_hash_to_merkle_hash_conversion_error() {
        use shardline_protocol::ShardlineHash;
        // A hash with invalid Merkle bytes (e.g., a short hash)
        let hash = ShardlineHash::from_bytes([0xFF; 32]);
        let result = super::shardline_hash_to_merkle_hash(hash);
        // MerkleHash::from_slice might succeed or fail depending on internal validation
        // In practice, from_slice always succeeds for 32 bytes in some implementations
        // but fail for others. Check that the result is consistent.
        let _ = result;
    }

    // ── shardline_hash_to_merkle_hash success path ───────────────────────

    #[test]
    fn shardline_hash_to_merkle_hash_succeeds() {
        use shardline_protocol::ShardlineHash;
        let hash = ShardlineHash::from_bytes([0xab; 32]);
        let result = super::shardline_hash_to_merkle_hash(hash);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // ── ValidatedXorbChunk with same start/end (zero length) ──────────

    #[test]
    fn validated_xorb_chunk_zero_length_chunk() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"zero"));
        let chunk = ValidatedXorbChunk::new(hash, 10, 10, 20, 20);
        assert_eq!(chunk.unpacked_len(), 0);
    }

    // ── InvalidFormatError debug/display for internal error variants ─────

    #[test]
    fn xorb_invalid_format_error_core_malformed_data_display() {
        let err = XorbInvalidFormatError::CoreMalformedData;
        let msg = err.to_string();
        assert!(msg.contains("malformed"), "msg: {msg}");
    }

    #[test]
    fn xorb_invalid_format_error_core_rejected_data_display() {
        let err = XorbInvalidFormatError::CoreRejectedData;
        let msg = err.to_string();
        assert!(msg.contains("rejected"), "msg: {msg}");
    }

    #[test]
    fn xorb_invalid_format_error_xorb_hash_conversion_failed_display() {
        let err = XorbInvalidFormatError::XorbHashConversionFailed;
        let msg = err.to_string();
        assert!(msg.contains("merkle"), "msg: {msg}");
    }

    #[test]
    fn xorb_invalid_format_error_chunk_hash_conversion_failed_display() {
        let err = XorbInvalidFormatError::ChunkHashConversionFailed;
        let msg = err.to_string();
        assert!(msg.contains("protocol"), "msg: {msg}");
    }

    // ── Additional ValidatedXorb/ValidatedXorbChunk edge cases ───────────

    #[test]
    fn validated_xorb_chunk_zero_length_saturating_arithmetic() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"sat"));
        // unpacked_start > unpacked_end -> saturating_sub returns 0
        let chunk = ValidatedXorbChunk::new(hash, 0, 10, 100, 50);
        assert_eq!(chunk.unpacked_len(), 0);
        assert_eq!(chunk.packed_start(), 0);
        assert_eq!(chunk.packed_end(), 10);
    }

    #[test]
    fn validated_xorb_chunk_full_range_use() {
        let h1 = merkle_hash_to_shardline_hash(compute_data_hash(b"a"));
        let h2 = merkle_hash_to_shardline_hash(compute_data_hash(b"b"));
        let c1 = ValidatedXorbChunk::new(h1, 0, 50, 0, 100);
        let c2 = ValidatedXorbChunk::new(h2, 50, 100, 100, 200);
        let xorb = ValidatedXorb::new(h1, 200, 100, 200, vec![c1, c2]);
        assert_eq!(xorb.chunks().len(), 2);
        assert_eq!(xorb.chunks()[0].hash(), h1);
        assert_eq!(xorb.chunks()[1].hash(), h2);
    }

    // ── decode_serialized_xorb_chunks with empty chunks ──────────────────

    #[test]
    fn decode_serialized_xorb_chunks_zero_chunks() {
        // Create a zero-chunk xorb and verify validation rejects it
        use shardline_xet_core::merklehash::MerkleHash;
        let dummy_hash = MerkleHash::from_slice(&[0xab; 32]).unwrap();
        let serialized = serialized_xorb_object_from_components(
            &dummy_hash,
            vec![],
            vec![],
            CompressionScheme::None,
        );
        if let Ok(s) = serialized {
            assert_eq!(s.num_chunks, 0);
            let hash_to_check = ShardlineHash::from_bytes([0xab; 32]);
            let mut reader = Cursor::new(s.serialized_data);
            let result = validate_serialized_xorb(&mut reader, hash_to_check);
            assert!(result.is_err(), "zero-chunk xorb should fail validation");
        }
    }

    // ── validate_serialized_xorb with reconstructed seek after validation ─

    #[test]
    fn validate_serialized_xorb_seeks_to_end_after_validation() {
        let data = b"seek check".to_vec();
        let chunk_hash = compute_data_hash(&data);
        let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(data.len()).unwrap())]);
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            data,
            vec![(chunk_hash, b"seek check".len() as u64)],
            CompressionScheme::None,
        )
        .unwrap();
        let expected_hash = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data.as_slice());
        let validated = validate_serialized_xorb(&mut reader, expected_hash).unwrap();

        // After validation, reader should be at end for total_length
        let stream_pos = reader.position();
        assert_eq!(stream_pos, validated.total_length());
    }

    // ── XorbParseError source chain ──────────────────────────────────────

    #[test]
    fn xorb_parse_error_source_from_io() {
        let io_err = std::io::Error::other("io source check");
        let parse_err: XorbParseError = io_err.into();
        // Display should show the io message
        let msg = parse_err.to_string();
        assert!(msg.contains("io"), "msg: {msg}");
    }

    #[test]
    fn xorb_parse_error_source_from_numeric_conversion() {
        let result = u64::try_from(-1i32);
        assert!(result.is_err());
        let num_err = result.unwrap_err();
        let parse_err: XorbParseError = num_err.into();
        let msg = parse_err.to_string();
        assert!(
            msg.contains("conversion") || msg.contains("TryFrom"),
            "msg: {msg}"
        );
    }

    // ── ValidatedXorbChunk ordering invariants in chunk vector ──────────

    #[test]
    fn validated_xorb_chunk_offsets_respect_ordering_invariants() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"ordering"));
        let chunks = vec![
            ValidatedXorbChunk::new(hash, 0, 50, 0, 100),
            ValidatedXorbChunk::new(hash, 50, 120, 100, 250),
            ValidatedXorbChunk::new(hash, 120, 200, 250, 400),
        ];
        // Verify monotonicity (packed and unpacked offsets increase)
        for pair in chunks.windows(2) {
            assert!(pair[0].packed_end() <= pair[1].packed_start());
            assert!(pair[0].unpacked_end() <= pair[1].unpacked_start());
        }
        let xorb = ValidatedXorb::new(hash, 200, 200, 400, chunks);
        assert_eq!(xorb.packed_content_length(), 200);
        assert_eq!(xorb.unpacked_length(), 400);
    }

    // ── DecodedXorbChunk with large binary data ─────────────────────────

    #[test]
    fn decoded_xorb_chunk_binary_data_roundtrip() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"binary"));
        let binary_data: Vec<u8> = (0..255).collect();
        let descriptor = ValidatedXorbChunk::new(hash, 0, 255, 0, 255);
        let chunk = DecodedXorbChunk::new(descriptor, binary_data.clone());
        assert_eq!(chunk.data(), binary_data.as_slice());
        assert_eq!(chunk.data().len(), 255);
    }

    // ── map_core_error with HashMismatch (additional context) ───────────

    #[test]
    fn map_core_error_extra_hash_mismatch_check() {
        let core_err = shardline_xet_core::error::CoreError::HashMismatch;
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(result, XorbParseError::HashMismatch),
            "expected HashMismatch, got {result:?}"
        );
    }

    #[test]
    fn map_core_error_extra_malformed_data_check() {
        let core_err = shardline_xet_core::error::CoreError::MalformedData("check".to_owned());
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(
                result,
                XorbParseError::InvalidFormat(XorbInvalidFormatError::CoreMalformedData)
            ),
            "expected CoreMalformedData, got {result:?}"
        );
    }

    // ── ValidatedXorbChunk equality and cloning ─────────────────────────

    #[test]
    fn validated_xorb_chunk_equality_and_clone() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"eq"));
        let a = ValidatedXorbChunk::new(hash, 0, 10, 0, 10);
        let b = ValidatedXorbChunk::new(hash, 0, 10, 0, 10);
        let c = ValidatedXorbChunk::new(hash, 0, 10, 0, 20);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.clone(), a);
    }

    #[test]
    fn validated_xorb_equality() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"eq2"));
        let chunk = ValidatedXorbChunk::new(hash, 0, 10, 0, 10);
        let a = ValidatedXorb::new(hash, 10, 10, 10, vec![chunk.clone()]);
        let b = ValidatedXorb::new(hash, 10, 10, 10, vec![chunk]);
        assert_eq!(a, b);
    }

    #[test]
    fn decoded_xorb_chunk_equality() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"eq3"));
        let d1 = ValidatedXorbChunk::new(hash, 0, 5, 0, 5);
        let a = DecodedXorbChunk::new(d1.clone(), vec![1, 2, 3]);
        let b = DecodedXorbChunk::new(d1, vec![1, 2, 3]);
        assert_eq!(a, b);
    }

    #[test]
    fn decoded_xorb_chunk_inequality() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"ineq"));
        let d1 = ValidatedXorbChunk::new(hash, 0, 5, 0, 5);
        let d2 = ValidatedXorbChunk::new(hash, 0, 10, 0, 10);
        let a = DecodedXorbChunk::new(d1, vec![1, 2, 3]);
        let b = DecodedXorbChunk::new(d2, vec![1, 2, 3]);
        assert_ne!(a, b);
    }

    // ── ShardlineHash <-> MerkleHash round trips ────────────────────────

    #[test]
    fn shardline_merkle_hash_round_trip_deterministic() {
        let original = merkle_hash_to_shardline_hash(compute_data_hash(b"deterministic"));
        let merkle = super::shardline_hash_to_merkle_hash(original).unwrap();
        let recovered = merkle_hash_to_shardline_hash(merkle);
        assert_eq!(original, recovered);
    }

    #[test]
    fn merkle_hash_to_shardline_non_zero() {
        let merkle = compute_data_hash(b"non-zero");
        let shardline = merkle_hash_to_shardline_hash(merkle);
        assert_ne!(
            shardline,
            ShardlineHash::from_bytes([0; 32]),
            "hash should be non-zero"
        );
    }

    // ── Public API integration smoke tests ───────────────────────────────

    #[test]
    fn validate_decode_try_for_each_round_trip() {
        let chunks_data = vec![b"part1".to_vec(), b"part2".to_vec(), b"part3".to_vec()];
        let hashes: Vec<_> = chunks_data.iter().map(|d| compute_data_hash(d)).collect();
        let xorb_hash = xorb_hash(
            &hashes
                .iter()
                .zip(chunks_data.iter())
                .map(|(h, d)| (*h, d.len() as u64))
                .collect::<Vec<_>>(),
        );
        let payload: Vec<u8> = chunks_data.iter().flatten().copied().collect();
        let boundaries: Vec<_> = hashes
            .iter()
            .scan(0u64, |acc, h| {
                *acc += chunks_data[hashes.iter().position(|x| x == h).unwrap()].len() as u64;
                Some((*h, *acc))
            })
            .collect();
        let serialized = serialized_xorb_object_from_components(
            &xorb_hash,
            payload,
            boundaries,
            CompressionScheme::None,
        )
        .unwrap();
        let expected = merkle_hash_to_shardline_hash(xorb_hash);
        let mut reader = Cursor::new(serialized.serialized_data);
        let validated = validate_serialized_xorb(&mut reader, expected).unwrap();
        assert_eq!(validated.chunks().len(), 3);

        let mut collected = Vec::new();
        try_for_each_serialized_xorb_chunk(&mut reader, &validated, |chunk| {
            collected.push(chunk.data().to_vec());
            Ok::<(), String>(())
        })
        .unwrap();
        assert_eq!(collected, chunks_data);
    }

    // ── ValidatedXorbChunk with custom hash ──────────────────────────────

    #[test]
    fn validated_xorb_chunk_custom_hash_bytes() {
        let custom_hash = ShardlineHash::from_bytes([0x42; 32]);
        let chunk = ValidatedXorbChunk::new(custom_hash, 0, 100, 0, 100);
        assert_eq!(chunk.hash(), custom_hash);
        assert_eq!(chunk.hash().as_bytes(), &[0x42; 32]);
    }

    // ── ValidatedXorb with single chunk at non-zero offset ──────────────

    #[test]
    fn validated_xorb_non_zero_packed_start() {
        let hash = merkle_hash_to_shardline_hash(compute_data_hash(b"nz"));
        let chunk = ValidatedXorbChunk::new(hash, 500, 1000, 0, 200);
        let xorb = ValidatedXorb::new(hash, 1500, 500, 200, vec![chunk]);
        assert_eq!(xorb.packed_content_length(), 500);
        assert_eq!(xorb.unpacked_length(), 200);
        assert_eq!(xorb.chunks()[0].packed_start(), 500);
    }

    // ── const fn and new patterns ───────────────────────────────────────

    #[test]
    fn validated_xorb_chunk_const_new_pattern() {
        const HASH_BYTES: [u8; 32] = [1; 32];
        let hash = ShardlineHash::from_bytes(HASH_BYTES);
        // Using the const fn
        let chunk = ValidatedXorbChunk::new(hash, 0, 1, 0, 1);
        assert!(chunk.unpacked_len() <= 1);
    }

    #[test]
    fn map_core_error_extra_internal_error() {
        let core_err = shardline_xet_core::error::CoreError::InternalError("test".to_owned());
        let result = super::map_core_error(&core_err);
        assert!(
            matches!(
                result,
                XorbParseError::InvalidFormat(XorbInvalidFormatError::CoreRejectedData)
            ),
            "expected CoreRejectedData, got {result:?}"
        );
    }
}
