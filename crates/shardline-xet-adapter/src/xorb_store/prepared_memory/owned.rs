//! Own native request bytes and check cancellation between validation reads.
use super::{ObjectIntegrity, PreparedXorbMemory, XetAdapterError, chunk_hash, parse_xet_hash_hex};
use crate::{checkpoint_reader::CheckpointReader, validate_serialized_xorb};
use shardline_xet_core::xorb_object::reconstruct_xorb_with_footer_from_reader;
use std::io::Cursor;

/// Prepare an owned native request without staging files or cloning canonical
/// input. The caller must retain memory/CPU admission until this worker stops.
///
/// # Errors
/// Returns malformed-content, content-hash, length or cancellation errors.
pub fn prepare_owned_xorb_memory_with_checkpoint(
    expected_hash: &str,
    mut bytes: Vec<u8>,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<PreparedXorbMemory, XetAdapterError> {
    checkpoint()?;
    let expected = parse_xet_hash_hex(expected_hash)?;
    let validation = validate_serialized_xorb(
        &mut CheckpointReader {
            reader: &mut Cursor::new(&bytes),
            checkpoint,
        },
        expected,
    );
    let validated = match validation {
        Ok(validated) => validated,
        Err(crate::XorbParseError::Io(error))
            if error.kind() != std::io::ErrorKind::UnexpectedEof =>
        {
            return Err(XetAdapterError::Io(error));
        }
        Err(_) => {
            let declared = crate::xorb::declared_footerless_unpacked_length(&bytes)?;
            if declared > crate::xorb::MAX_XORB_UNPACKED_BYTES {
                return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
            }
            // At most 8192 members bound the footer below 1 MiB. Reserve its
            // room up front instead of doubling a nearly full payload buffer.
            let capacity = bytes
                .len()
                .checked_add(1024 * 1024)
                .ok_or(XetAdapterError::Overflow)?;
            let mut normalized = Vec::with_capacity(capacity);
            let raw_length = u64::try_from(bytes.len())?;
            let (canonical, computed) = reconstruct_xorb_with_footer_from_reader(
                &mut normalized,
                &mut CheckpointReader {
                    reader: &mut Cursor::new(&bytes),
                    checkpoint,
                },
                raw_length,
            )
            .map_err(super::super::normalization::normalization_error)?;
            checkpoint()?;
            if parse_xet_hash_hex(&computed.hex())? != expected {
                return Err(XetAdapterError::XorbHashMismatch);
            }
            // Reconstruction authenticated each decoded member and computed
            // its root, then generated the footer from those verified members.
            // Carry that result; never decode/hash the same owned bytes again.
            let validated = crate::xorb::metadata_from_verified_xorb(
                expected,
                u64::try_from(normalized.len())?,
                &canonical,
            )?;
            if validated.packed_content_length() != raw_length {
                return Err(XetAdapterError::InvalidSerializedXorb);
            }
            bytes = normalized;
            validated
        }
    };
    checkpoint()?;
    let integrity = ObjectIntegrity::new(chunk_hash(&bytes), u64::try_from(bytes.len())?);
    checkpoint()?;
    Ok(PreparedXorbMemory {
        expected_hash: expected_hash.to_owned(),
        bytes,
        validated,
        integrity,
    })
}
