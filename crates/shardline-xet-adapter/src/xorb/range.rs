//! Decode only selected members after complete source validation.

use std::{
    io::{Read, Seek},
    ops::Range,
};

use super::{
    DecodedXorbChunk, ValidatedXorb, XorbInvalidFormatError, XorbParseError, XorbVisitError,
};

/// Decodes and rehashes a nonempty member range of a fully validated Xorb.
///
/// The caller must first validate the complete source with
/// [`super::validate_serialized_xorb`]. This second pass seeks directly to the
/// selected members, retaining their structural, length and content-hash checks.
/// It does not replace full-source validation or authenticate an arbitrary index.
///
/// # Errors
/// Returns an error for an empty/out-of-bounds range, changed selected bytes,
/// invalid structure, I/O failure or a rejected visitor.
pub fn try_for_each_serialized_xorb_chunk_range<R, F, VisitorError>(
    reader: &mut R,
    validated: &ValidatedXorb,
    range: Range<usize>,
    visitor: F,
) -> Result<(), XorbVisitError<VisitorError>>
where
    R: Read + Seek,
    F: FnMut(DecodedXorbChunk) -> Result<(), VisitorError>,
{
    let descriptors = validated
        .chunks()
        .get(range)
        .filter(|chunks| !chunks.is_empty())
        .ok_or_else(|| {
            XorbParseError::from(XorbInvalidFormatError::ChunkPayloadMetadataMismatch)
        })?;
    let end = descriptors
        .last()
        .ok_or_else(|| XorbParseError::from(XorbInvalidFormatError::ChunkPayloadMetadataMismatch))?
        .packed_end();
    let start = descriptors
        .first()
        .ok_or_else(|| XorbParseError::from(XorbInvalidFormatError::ChunkPayloadMetadataMismatch))?
        .packed_start();
    super::try_for_each_serialized_xorb_chunk_inner(reader, descriptors, start, end, visitor, true)
}

#[cfg(test)]
mod tests;
